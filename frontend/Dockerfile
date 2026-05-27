FROM nginx:alpine

# Remove a configuração default
RUN rm /etc/nginx/conf.d/default.conf

# Copia nossa configuração do Nginx (com proxy para o backend)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copia os arquivos estáticos
COPY index.html /usr/share/nginx/html/index.html
COPY style.css  /usr/share/nginx/html/style.css
COPY app.js     /usr/share/nginx/html/app.js

EXPOSE 80
