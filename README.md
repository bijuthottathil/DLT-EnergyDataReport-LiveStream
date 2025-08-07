# Databricks Delta Live Tables (DLT) pipeline. This pipeline simulates a real-time power meter ingestion system using a simulator
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

10.   <img width="1099" height="603" alt="image" src="https://github.com/user-attachments/assets/3da8a551-29bd-480f-8176-b9f03da7b303" />

11. <img width="1246" height="740" alt="image" src="https://github.com/user-attachments/assets/c8b4b9fd-b692-4058-8523-fe8a19b85844" />

12  <img width="1306" height="789" alt="image" src="https://github.com/user-attachments/assets/4f8430e5-e9c6-4cf3-bb35-632ac53ee6a4" />  

13 <img width="1216" height="494" alt="image" src="https://github.com/user-attachments/assets/ce1021f9-ccd6-4587-b350-017c21a839af" />  

14  <img width="1305" height="822" alt="image" src="https://github.com/user-attachments/assets/bc0d195d-27c4-4115-9f2e-a2fbbfba24ae" />  

15. <img width="1334" height="862" alt="image" src="https://github.com/user-attachments/assets/270c5232-9523-4f64-9341-62926bf4d968" />

16.  <img width="1398" height="793" alt="image" src="https://github.com/user-attachments/assets/394f27f6-bf29-4269-9b46-c323de803edf" />
17.  <img width="1472" height="901" alt="image" src="https://github.com/user-attachments/assets/00e8fc56-7023-4e59-9148-1960a85252bd" />
18.  <img width="1186" height="826" alt="image" src="https://github.com/user-attachments/assets/8bd4c4a5-b913-4dc5-89c2-db1b90c7b75a" />
19.  <img width="1073" height="879" alt="image" src="https://github.com/user-attachments/assets/c762f53e-1c47-4907-9018-e3f7f4a185af" />
20.  <img width="1192" height="802" alt="image" src="https://github.com/user-attachments/assets/3919f758-cfa2-4c8b-b1e8-84f1c5caed49" />
21.  <img width="1002" height="475" alt="image" src="https://github.com/user-attachments/assets/0266675b-1157-411f-9a46-47dd310b362b" />





# Now I am going to connect to Aggregated table in Databricks to PowerBI desktop to create a report

 <img width="1880" height="781" alt="image" src="https://github.com/user-attachments/assets/b7399c3c-a7ca-4d94-b6f7-d8ecbb5a8479" />

  <img width="1651" height="864" alt="image" src="https://github.com/user-attachments/assets/1df121f8-9d9b-4357-a5cd-12a297682cc9" />  

  <img width="1649" height="959" alt="image" src="https://github.com/user-attachments/assets/aa094b2e-bdaa-42ce-a35d-e5adb9396308" />

  Note:- you can get above details from SQL Warehouse connection properties

  Before loading below screen, you will get option for authentication, please create Personal Access Token from Databricks-->settings --> Developer Section

<img width="1699" height="978" alt="image" src="https://github.com/user-attachments/assets/f33f09ca-09a7-4775-8da2-73c17318196a" />  

<img width="1655" height="941" alt="image" src="https://github.com/user-attachments/assets/4d363842-e7fb-4a62-8c8a-9ce293076f41" />    
<img width="1685" height="1004" alt="image" src="https://github.com/user-attachments/assets/81013052-ce8c-4c41-8bab-1106a4e9eaea" />

Just for testing, I pulled aggregation data in canvas

<img width="1332" height="761" alt="image" src="https://github.com/user-attachments/assets/2d77e676-cc6e-4790-9121-ad93217d7cf5" />

I am still working on the PowerBI report. Final version will be updated in 2 days



  

