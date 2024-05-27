# Use uma imagem base oficial do Python
FROM python:3.9

# Defina o diretório de trabalho
WORKDIR /docker

# Copie os arquivos de requisitos e instale as dependências
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copie o código da função
COPY jump-arquivo-consolidado.py jump-arquivo-consolidado.py

# Defina o comando de entrada para a função
CMD ["python", "jump-arquivo-consolidado.py"]
