 FROM python:3.3.3

 RUN pip3 install --upgrade pip
 RUN pip3 install numpy
 RUN pip3 install requests
 RUN pip3 install pandas
 RUN pip3 install xlrd
 RUN pip3 install xlwt
 RUN pip3 install openpyxl
 RUN pip3 install datetime
 RUN pip3 install py-postgresql
 CMD ["python3"]
 
