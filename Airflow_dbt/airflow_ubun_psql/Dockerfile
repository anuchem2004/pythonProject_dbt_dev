FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ARG AIRFLOW_VERSION=2.3.3

RUN apt-get update && apt-get install -y tzdata
RUN apt-get install python3 python3-pip sudo -y
RUN apt-get install gnupg2 vim  -y
RUN apt-get install postgresql postgresql-contrib -y
RUN apt-get install -y rabbitmq-server
RUN    apt-get update\
    && apt-get -y install gcc libffi-dev zlib1g-dev zip unzip telnet dnsutils net-tools vim nano wget curl \
    && apt-get -y install make libssl-dev

RUN useradd -c 'airflow' -m -s /bin/bash   airflow
RUN echo "airflow:airflow"|chpasswd
#RUN usermod -aG sudo airflow
RUN echo "host all  all    0.0.0.0/0  md5" >>/etc/postgresql/12/main/pg_hba.conf
RUN echo "listen_addresses= '*'" >> /etc/postgresql/12/main/postgresql.conf
#RUN pip3 install  apache-airflow[postgres,celery,rabbitmq]==2.3.3
RUN pip3 install virtualenv
RUN pip3 install markupsafe==1.1.1 flask==1.1.4 click==8.0.0  jinja2==2.10.1 
RUN pip3 install dbt-core dbt-redshift dbt-postgres
RUN pip3 install Faker psycopg2_binary Werkzeug flask_cors supervisor
RUN mkdir /tmp/dbt-orchestration
COPY dbt-orchestration /tmp/dbt-orchestration/
#RUN ln -s  /usr/local/lib/python3.10/dist-packages/airflow/example_dags/* /root/airflow/dags/
COPY supervisord.conf  /etc/supervisord.conf
COPY rabbitmq.ini /etc/supervisord.d/rabbitmq.ini
COPY rabbitmqadmin.py /usr/bin/rabbitmqadmin.py
COPY airflow.ini /etc/supervisord.d/airflow.ini
RUN mkdir -p /usr/local/airflow/dags
COPY data-generator-one-row.py  data-generator-random-rows.py simpledag-Pen.py config.json /usr/local/airflow/dags/
RUN mkdir -p /usr/local/airflow/logs
RUN mkdir -p /usr/local/airflow/plugins
RUN chmod 777 /bin/rabbitmqadmin.py
RUN mkdir -p /var/run/supervisor
RUN mkdir -p /var/log/supervisor
RUN chown -R airflow.airflow /usr/local/airflow/
RUN chown -R airflow.airflow /tmp/dbt-orchestration/

RUN python3 -m pip install -U pip setuptools wheel \
    && pip uninstall  -y chardet\
    && pip install Cython \
    && pip install pika\
    && pip install psycopg2-binary\
    && pip install pytz  \
    && pip install  urllib3 pyOpenSSL --force --upgrade \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install supervisor \
    && pip install werkzeug==0.15.0
RUN  apt-get install -y g++  libsasl2-dev
RUN sudo apt-get install --reinstall libpq-dev
RUN pip3 install apache-airflow[s3,crypto,celery,postgres,hive,jdbc,snowflake,databricks]==${AIRFLOW_VERSION}
RUN apt-get install mlocate -y

RUN sed -i -e '/sql_alchemy_conn =/ s/= .*/= postgresql+psycopg2:\/\/airflow:airflow@localhost:5432\/airflow/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 

RUN sed -i -e '/executor =/ s/= .*/= CeleryExecutor/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 
RUN sed -i -e '/broker_url =/ s/= .*/= amqp:\/\/airflow:airflow@localhost:5672/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 

RUN sed -i -e '/result_backend =/ s/= .*/= db+postgresql:\/\/airflow:airflow@localhost\/airflow/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 
RUN sed -i -e '/load_examples =/ s/= .*/= False/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 
RUN sed -i -e '/dags_folder =/ s/= .*/= \/usr\/local\/airflow\/dags/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 
RUN sed -i -e '/base_log_folder =/ s/= .*/= \/usr\/local\/airflow\/logs/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg
RUN sed -i -e '/dag_dir_list_interval =/ s/= .*/= 60/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg 
RUN sed -i -e '/fernet_key =/ s/= .*/= __xYFdYeIdgohydkqLiZgXUDkZ1xfPPHikNTDhpPX8I=/' /usr/local/lib/python3.8/dist-packages/airflow/config_templates/default_airflow.cfg
# ENV AIRFLOW_VAR_DATABASE=postgres
# ENV AIRFLOW_VAR_HOSTNAME=www.americaniche.com
# ENV AIRFLOW_VAR_PORT_ID=33306
# ENV AIRFLOW_VAR_PWD=password
# ENV AIRFLOW_VAR_USERNAME=postgres
# ENV AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800
ENV AIRFLOW_HOME=/usr/local/airflow
RUN usermod -aG sudo airflow
#RUN chown -R airflow.airflow /home/airflow/
COPY dbt-scheduler.py /usr/local/airflow/dags/
COPY entrypoint.sh /bin/entrypoint
RUN chmod 777 /bin/entrypoint
CMD "/bin/entrypoint"
