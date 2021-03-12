#main image
FROM python:3.8.8-slim AS main
RUN pip install pandas
RUN pip install cython
RUN pip install cymem
RUN pip install pyyaml
RUN python -m pip install --upgrade pip
RUN mkdir -p /app
COPY main.js /app/
RUN mkdir /output
VOLUME /output
EXPOSE 8080
CMD ["node", "/app/main.js"]