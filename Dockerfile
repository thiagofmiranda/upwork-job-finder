FROM public.ecr.aws/lambda/python:3.11

# Instalações do sistema
RUN yum install -y unzip wget xz nss \
 && yum clean all

# Instala Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Instala o Playwright + browsers
RUN pip install playwright \
 && playwright install --with-deps chromium

# Copia código
COPY app/ ${LAMBDA_TASK_ROOT}

# Define o handler
CMD ["lambda_function.lambda_handler"]
