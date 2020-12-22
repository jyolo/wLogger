FROM centos
RUN yum list | grep python \
    && yum -y install python38 \ 
    && yum -y install git \ 
    && cd / &&  git clone https://github.com/jyolo/wLogger \ 
    && cd /wLogger \ 
    && pip3 install -r requirements.txt \ 
    && echo "/usr/bin/python3 /wLogger/main.py \$@" > run.sh  
    

EXPOSE 5000


ENTRYPOINT ["/bin/bash","/wLogger/run.sh"]
