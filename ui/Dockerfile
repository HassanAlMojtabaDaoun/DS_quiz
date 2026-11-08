# The bundle in dist/ is prebuilt (npm run build), so no Node.js is needed
# to run the UI. To rebuild: cd ui && npm install && npm run build
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY dist/ /usr/share/nginx/html/
