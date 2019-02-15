FROM alexeiled/docker-oracle-xe-11g

ADD usermanage.sql /tmp/
ADD surechembl.sql /tmp/
ADD fireitup.sh /
RUN chmod 755 /fireitup.sh

ENTRYPOINT [ "/fireitup.sh" ]