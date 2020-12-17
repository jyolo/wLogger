FROM centos
RUN yum -y install python38 \ 
    && yum -y install wget \ 
    && cd \ && wget -O wLogger-1.3.tar.gz "https://codeload.github.com/jyolo/wLogger/tar.gz/v1.3" \ 
    && tar -zxvf wLogger-1.3.tar.gz && cd wLogger-1.3 \
    && pip3 install -r requirements.txt
