# Imagem base oficial e leve do Node
FROM node:20-alpine

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia primeiro o package.json para aproveitar cache de camadas
COPY package*.json ./

# Instala apenas dependências de produção
RUN npm install --omit=dev

# Copia o restante do código-fonte
COPY . .

# Porta exposta pelo Express
EXPOSE 3000

# Comando padrão
CMD ["node", "server.js"]
