 FROM python:3
 ENV PYTHONUNBUFFERED 1
 RUN mkdir rep
 WORKDIR rep
 ADD requirements.txt rep
 RUN pip3 install -r requirements.txt
 
