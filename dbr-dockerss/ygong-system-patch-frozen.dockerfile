RUN add-apt-repository --remove 'databricks-ml'
RUN apt list zulu8 --installed | grep zulu8 && sudo apt-mark hold zulu8 || echo "no zulu 8 installation found"
RUN apt list zulu11 --installed | grep zulu11 && sudo apt-mark hold zulu11 || echo "no zulu 11 installation found"
RUN apt-get --allow-releaseinfo-change-origin update && apt-get -y upgrade
RUN unattended-upgrade -d

