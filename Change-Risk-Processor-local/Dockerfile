FROM node:22.5.1


# Install gcompat to provide ld-linux-x86-64.so.2
# RUN apk add --no-cache libc6-compat gcompat build-base python3

WORKDIR /usr/app
COPY . .
RUN npm cache clean --force && npm install
VOLUME ["/usr/app/certs"]
EXPOSE 8080
CMD ["npm", "start"]