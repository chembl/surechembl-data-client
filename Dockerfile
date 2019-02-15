FROM python:2.7

LABEL maintainer="arcila@ebi.ac.uk"

ENV TNS_ADMIN=/usr/lib/oracle/18.3/client64 \
    ORACLE_HOME=/usr/lib/oracle/18.3/client64 \
    PATH=$PATH:/usr/lib/oracle/18.3/client64/bin \
    SURE_DATA_CLIENT=/opt/suredataclient

WORKDIR ${SURE_DATA_CLIENT}
ADD . ${SURE_DATA_CLIENT}

SHELL ["/bin/bash", "-c"]

RUN apt-get -qq update && apt-get -y -qq upgrade && \
    apt-get -y -qq install alien wget libaio1 gettext git locales && \
    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen && \
    locale-gen && \
    echo "export LC_ALL=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANG=en_US.UTF-8" >> ~/.bashrc && \
    echo "export LANGUAGE=en_US.UTF-8" >> ~/.bashrc && \
    alien ${SURE_DATA_CLIENT}/Docker/data-client/oracle-instantclient18.3-* && \
    rm -f ${SURE_DATA_CLIENT}/Docker/data-client/oracle-instantclient18.3-* && \
    dpkg -i oracle-instantclient*.deb && \
    echo /usr/lib/oracle/18.3/client64/lib > /etc/ld.so.conf.d/oracle-instantclient18.3.conf && \
    ldconfig

RUN mv ${SURE_DATA_CLIENT}/Docker/data-client/tnsnames.ora /usr/lib/oracle/18.3/client64

RUN pip install cx_Oracle sqlalchemy psycopg2

RUN chmod 755 ${SURE_DATA_CLIENT}/Docker/data-client/fireitup.sh

EXPOSE 8000

ENTRYPOINT [ "./Docker/data-client/fireitup.sh" ]