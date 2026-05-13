FROM nginx:alpine
EXPOSE 80
ADD ./nginx/nginx-dev.conf /etc/nginx/nginx.conf
RUN rm -f /etc/nginx/conf.d/default.conf
