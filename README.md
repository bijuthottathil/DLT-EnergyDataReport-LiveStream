# Databricks Delta Live Tables (DLT) pipeline designed for an energy company to manage and report on power usage data using the Medallion Architecture (Bronze → Silver → Gold).
<img width="828" height="549" alt="image" src="https://github.com/user-attachments/assets/b369c117-dcc9-4160-9dc6-0852d208aa72" />

1. Create external locations in S3 buckets
   <img width="1887" height="488" alt="image" src="https://github.com/user-attachments/assets/b0d0f2e8-2aec-44a9-9cf4-ec5b406b7e3a" />

2. Created power-catalog and 3 Schema's manually  <img width="1885" height="660" alt="image" src="https://github.com/user-attachments/assets/7b10a1aa-521f-46ff-a4d7-d008792507af" />

3. Create ETL Pipe Line from here <img width="1889" height="568" alt="image" src="https://github.com/user-attachments/assets/43624973-b2ba-4738-a904-57cb834e7c5b" />  

4. <img width="1714" height="925" alt="image" src="https://github.com/user-attachments/assets/c2e7e6aa-ec5a-40b1-80fd-5df7f0bbcaf7" />

5.  Already I have created a project. Once it is created , you will get transformations folder. You can create new files (py or sql) for DLT pipeline. I selecte Python option here  
   <img width="1899" height="718" alt="image" src="https://github.com/user-attachments/assets/f61efb5e-fafa-4527-981d-343ee29acf0d" />

6. I have provided my code in git repository, same code is given below
   
Important Note:- I am using a simulator to generate Json files with Power Meter readings . This python simulator will generated records in an external volume created in S3 bucket

7. <img width="1896" height="844" alt="image" src="https://github.com/user-attachments/assets/4c0a0b1f-9be3-40ec-b63d-791200ded483" />

8. I kept 2 csv files to load Customers details and Electricity Plan details in csv folder
   <img width="1890" height="799" alt="image" src="https://github.com/user-attachments/assets/5e1d403c-d9d1-4639-9ac7-66f27885de03" />

9. Json file content will look like this   <img width="1883" height="472" alt="image" src="https://github.com/user-attachments/assets/7e5500c9-5be1-4478-9a28-fb0c8174db9f" />


<img width="1880" height="781" alt="image" src="https://github.com/user-attachments/assets/b7399c3c-a7ca-4d94-b6f7-d8ecbb5a8479" />

  <img width="1651" height="864" alt="image" src="https://github.com/user-attachments/assets/1df121f8-9d9b-4357-a5cd-12a297682cc9" />  

  <img width="1649" height="959" alt="image" src="https://github.com/user-attachments/assets/aa094b2e-bdaa-42ce-a35d-e5adb9396308" />

  Note:- you can get above details from SQL Warehouse connection properties

  Before loading below screen, you will get option for authentication, please create Personal Access Token from Databricks-->settings --> Developer Section

<img width="1699" height="978" alt="image" src="https://github.com/user-attachments/assets/f33f09ca-09a7-4775-8da2-73c17318196a" />  

<img width="1655" height="941" alt="image" src="https://github.com/user-attachments/assets/4d363842-e7fb-4a62-8c8a-9ce293076f41" />    
<img width="1685" height="1004" alt="image" src="https://github.com/user-attachments/assets/81013052-ce8c-4c41-8bab-1106a4e9eaea" />

Just for testing, I pulled aggregation data in canvas

<img width="1897" height="912" alt="image" src="https://github.com/user-attachments/assets/977327a5-09c4-44fe-b55b-bb1cfe19e1d4" />


  

