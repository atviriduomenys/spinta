FROM nginx:alpine
EXPOSE 80
ADD ./nginx/nginx.conf /etc/nginx/
RUN rm -f /etc/nginx/conf.d/default.conf
