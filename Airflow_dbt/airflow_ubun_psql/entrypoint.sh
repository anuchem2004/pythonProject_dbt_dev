service postgresql start
su - postgres -c "psql --command \"CREATE USER airflow PASSWORD 'airflow';\""
su - postgres -c "psql --command \"CREATE DATABASE airflow;\""
su - postgres -c "psql --command \"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;\""
rabbitmq-server&
sleep 10
rabbitmq-plugins enable  rabbitmq_web_mqtt rabbitmq_web_mqtt_examples rabbitmq_web_stomp rabbitmq_web_stomp_examples rabbitmq_trust_store rabbitmq_top rabbitmq_management_agent rabbitmq_management rabbitmq_jms_topic_exchange rabbitmq_amqp1_0
sleep 5
rabbitmqadmin.py  declare user name=airflow  password=airflow  tags=administrator
rabbitmqadmin.py  declare queue name=airflow
rabbitmqadmin.py  declare permission vhost=/ user=airflow configure=.* write=.* read=.
rabbitmqctl stop
airflow db init
supervisord -c /etc/supervisord.conf
sleep 5
airflow users create    --username admin1  --firstname Peter --lastname Quill --role Admin  --email spiderman@superhero.org --password admin1
supervisorctl status
#airflow db init
#airflow standalone
tail -f "/dev/null"
