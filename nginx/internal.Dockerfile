FROM nginx:alpine
EXPOSE 8000
ADD ./nginx/nginx-internal.conf /etc/nginx/nginx.conf
RUN rm -f /etc/nginx/conf.d/default.conf
