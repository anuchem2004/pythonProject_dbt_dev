FROM ubuntu:18.04

RUN apt update

ENV DEBIAN_FRONTEND=noninteractive

RUN ln -fs /usr/share/zoneinfo/Europe/Budapest /etc/localtime

RUN apt install -y tzdata libterm-readline-gnu-perl

RUN dpkg-reconfigure -f noninteractive tzdata

RUN echo "mysql-server-8.0 mysql-server/root_password password secret" | debconf-set-selections \
    && echo "mysql-server-8.0 mysql-server/root_password_again password secret" | debconf-set-selections

RUN apt install -y mysql-server

RUN mysql -uroot -p -e 'USE mysql; UPDATE `user` SET `Host`="%" WHERE `User`="root" AND `Host`="localhost"; DELETE FROM `user` WHERE `Host` != "%" AND `User`="root"; FLUSH PRIVILEGES;'

RUN /etc/init.d/mysql start