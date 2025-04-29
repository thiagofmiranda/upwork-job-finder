import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
import pandas as pd
import duckdb
import asyncio
from app.scripts.utils import apply_job

load_dotenv(dotenv_path=".env")
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
STAGING_PATH = os.getenv("STAGING_PATH")
SENT_PATH = os.getenv("SENT_PATH")
APPLIED_PATH = os.getenv("APPLIED_PATH")


async def send_jobs(channel, already_sent_ids):
        
        # Processa novos jobs
        try:
            job_df = duckdb.query(f"""
                SELECT * FROM read_parquet('{STAGING_PATH}/*.parquet')
                WHERE apply = TRUE
                ORDER BY match_level;
            """).to_df()
        except:
            job_df = pd.DataFrame(columns=["job_id"])
        
        await channel.send(f"🖨️ Sending Step: Found {len(job_df)} jobs to send.")
        await channel.send(f"🖨️ Sending Step: Found {len(already_sent_ids)} jobs have been already sent.")
        
        job_df = job_df[job_df["job_id"].isin(already_sent_ids) == False]
        await channel.send(f"🖨️ Sending Step: Found {len(job_df)} new jobs to send.")

        for _, row in job_df.iterrows():
            embed = discord.Embed(
                title=row["job_title"],
                description=row["job_description"][:4096] if len(row["job_description"]) > 4096 else row["job_description"],
                color=discord.Color.yellow(),
                url=row["job_link"]
            )
            embed.add_field(name="Job ID", value=row["job_id"], inline=True)
            embed.add_field(name="Experience Level", value=row["job_type_level"], inline=True)
            embed.add_field(name="Duration", value=row["duration_label"], inline=True)
            embed.add_field(name="Fixed Price?", value="✅" if row["is_fixed_price"] else "❌", inline=True)
            embed.add_field(name="Match Level", value=row["match_level"], inline=True)
            embed.add_field(name="Reason", value=row["reason"], inline=False)

            await channel.send(embed=embed, view=JobView(row=row, APPLIED_PATH=APPLIED_PATH))

            # Adiciona aos enviados
            already_sent_ids.append(row["job_id"])
            file_name = f'{SENT_PATH}/jobs-sent.csv'
            row_df = pd.DataFrame([row])
            if os.path.exists(file_name):
                row_df.to_csv(file_name, mode='a', index=False, header=False)
            else:
                row_df.to_csv(file_name, mode='w', index=False, header=True)


async def run_core(channel, STAGING_PATH=STAGING_PATH, SENT_PATH=SENT_PATH, APPLIED_PATH=APPLIED_PATH):
    await channel.send("🚀 Running job process...")
    
    # Executa o scraper
    process = await asyncio.create_subprocess_exec(
    "python", "-u", "app/scripts/main.py",  # Adiciona o flag -u para desativar o buffering
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    env={**os.environ, "PYTHONUNBUFFERED": "1"}  # Garante que o subprocesso use saída não bufferizada
)

    # stdout, stderr = await process.communicate()
    # print(f"STDOUT: {stdout.decode()}")
    # print(f"STDERR: {stderr.decode()}")

    while True:
        line = await process.stdout.readline()
        if not line:
            break
        decoded_line = line.decode().strip()
        print(decoded_line)
        await channel.send(f"🖨️ {decoded_line}")

    await process.wait()

    # Carrega jobs já enviados
    already_sent_ids = pd.read_csv(f'{SENT_PATH}/jobs-sent.csv')['job_id'].tolist() if os.path.exists(f'{SENT_PATH}/jobs-sent.csv') else []

    await send_jobs(channel, already_sent_ids)
    
    await channel.send("✅ Job process finished!")


async def watch_jobs(channel, STAGING_PATH=STAGING_PATH, SENT_PATH=SENT_PATH, APPLIED_PATH=APPLIED_PATH):
    while True:
        await run_core(channel, STAGING_PATH, SENT_PATH, APPLIED_PATH)
        await asyncio.sleep(3600)  # Espera 1h até a próxima execução

intents = discord.Intents.default()
intents.message_content = True  # Enable message content intent

bot = commands.Bot(command_prefix="!", intents=intents)

class JobView(discord.ui.View):
    def __init__(self, row, APPLIED_PATH):
        super().__init__(timeout=None)  # Chama o init da View normalmente
        self.row = row  # Agora salva o row como atributo
        self.APPLIED_PATH = APPLIED_PATH  # Salva o caminho do diretório de aplicados

    async def apply_reply(self, interaction: discord.Interaction):
        application = apply_job(self.row)  # Usa o self.row

        # Salva o job aplicado no CSV
        row_df = pd.DataFrame([self.row])
        file_name = f'{self.APPLIED_PATH}/jobs-applied.csv'
        if os.path.exists(file_name):
            row_df.to_csv(file_name, mode='a', index=False, header=False)
        else:
            row_df.to_csv(file_name, mode='w', index=False, header=True)

        # Cria a thread com o máximo de tempo possível antes de arquivar
        thread = await interaction.channel.create_thread(
            name="Application reply",
            message=interaction.message,
            auto_archive_duration=10080  # 7 dias (máximo possível)
        )

        # Envia a aplicação na thread
        await thread.send(content=f"{interaction.user.mention}\n{application}")

    @discord.ui.button(label="Apply", style=discord.ButtonStyle.grey, emoji="😎")
    async def apply_button(self, interaction: discord.Interaction, button: discord.Button):
        await interaction.response.defer()

        await self.apply_reply(interaction)

        button.label = "Applied"
        button.emoji = "✅"
        button.style = discord.ButtonStyle.green
        button.disabled = True

        skip_button = [b for b in self.children if b.label == "Skip"][0]
        skip_button.disabled = True

        emb = interaction.message.embeds[0]
        emb.description = "You applied successfully!"
        emb.color = discord.Color.green()

        await interaction.edit_original_response(embed=emb, view=self)

    @discord.ui.button(label="Skip", style=discord.ButtonStyle.grey, emoji="❌")
    async def skip_button(self, interaction: discord.Interaction, button: discord.Button):
        await interaction.response.defer()

        button.label = "Skipped"
        button.style = discord.ButtonStyle.red
        button.emoji = "🙅🏻‍♂️"
        button.disabled = True

        apply_button = [b for b in self.children if b.label == "Apply"][0]
        apply_button.disabled = True

        emb = interaction.message.embeds[0]
        emb.color = discord.Color.red()
        emb.description = "You skipped the application!"
        await interaction.edit_original_response(embed=emb, view=self)


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user.name} ({bot.user.id})")
    guild = discord.Object(id=int(GUILD_ID))
    bot.tree.copy_global_to(guild=guild)
    await bot.tree.sync(guild=guild)
    print(f"Synced commands to guild: {guild.id}")

    channel = bot.get_channel(int(CHANNEL_ID))

    # Inicia a monitoração do CSV
    bot.loop.create_task(watch_jobs(channel, STAGING_PATH=STAGING_PATH, APPLIED_PATH=APPLIED_PATH))

# command to delete all messages in the channel
@bot.tree.command(name="delete_all", description="Deleta todas as mensagens")
@commands.has_permissions(manage_messages=True)
async def delete_all(ctx):
    channel = bot.get_channel(int(CHANNEL_ID))
    if channel:
        async for message in channel.history(limit=1000):
            await message.delete()
        await ctx.send("All messages deleted.")
    else:
        await ctx.send("Channel not found.")

@bot.tree.command(name="run_scraper", description="Run scraping")
@commands.has_permissions(administrator=True)
async def run_script(ctx):
    try:
        channel = bot.get_channel(int(CHANNEL_ID))
        await run_core(channel, STAGING_PATH, SENT_PATH, APPLIED_PATH)

    except Exception as e:
        await channel.send(f"⚠️ Exceção: {e}")

@bot.tree.command(name="resend_last_jobs", description="Send jobs")
@commands.has_permissions(administrator=True)
async def resend_last_jobs(ctx, n_last: int = 10):
    try:
        channel = bot.get_channel(int(CHANNEL_ID))

        already_sent_ids = pd.read_csv(f'{SENT_PATH}/jobs-sent.csv')['job_id'].tolist() if os.path.exists(f'{SENT_PATH}/jobs-sent.csv') else []
        # remover os últimos n_last ids da lista
        already_sent_ids = already_sent_ids[:-n_last] if len(already_sent_ids) > n_last else []
        await send_jobs(channel, already_sent_ids)
        
    except Exception as e:
        await channel.send(f"⚠️ Exceção: {e}")

bot.run(TOKEN)


