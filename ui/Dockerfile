# The bundle in dist/ is prebuilt (npm run build), so no Node.js is needed
# to run the UI. To rebuild: cd ui && npm install && npm run build
FROM nginx:1.27-alpine
# The nginx image fills ${API_UPSTREAM} in templates at start-up:
#   docker compose -> lb:80 (load balancer), Kubernetes -> gateway:8080
ENV API_UPSTREAM=gateway:8080
COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY dist/ /usr/share/nginx/html/
