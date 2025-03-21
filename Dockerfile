FROM apache/airflow:2.7.1
WORKDIR /opt/airflow
RUN id root
USER root 
# Переключаемся на админского пользователя, тк этого требует apt
RUN apt update && apt -y install procps default-jre 
# это требуется, чтобы в контейнере airflow мог запускаться spark-job (spark-submit)
USER airflow
 # Переключаемся обратно на юзера airflow, чтоб сам сервис работал корректно и не сломались пермишены
#  COPY ./dags/* ./dags/
#  COPY ./spark/* ./spark/ я не понял зачем это, ведь мы все равно монтирует папку и копирование затирается
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt