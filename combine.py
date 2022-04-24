import re
import pdfplumber
import os
from pdf_parser import data_extractor_numbers,data_extractor_alphanumeric,data_extractor_string
import psycopg2
from aws_lib_.aws_ocr_main import main_call
import sys
from PIL import Image


##conn = psycopg2.connect(database='postgres', user='postgres',password='password',host='localhost',port='5432')
##cursor = conn.cursor()

# Resize Image
# def resize_image(image_path):
#     if os.path.getsize(image_path) > 10000000:
#         foo = Image.open(image_path)
#         foo = foo.resize((800, 800), Image.ANTIALIAS)
#         foo.save(image_path, quality=95)
sys.path.append(r"C:\Python39\Lib\site-packages\aws_lib_")   #AWS

path = r"C:\Users\Sameer\Downloads\spun\spun\lohia_vendor"
os.chdir(path)
def read_file(path):
    data_dict = {}
    l = ['(', ')', '.', '/', '-', ',', '%','&']
    data = ""
    with pdfplumber.open(path) as pdf:
        pg = pdf.pages
        # print(pg)
        for i in range(len(pg)):
            txt = pg[i].extract_text()
            data = data+'\n--------------------------------------------new page------------------------------------------\n'+txt
        
        if 'Spun Micro Processing Pvt. Ltd.' in data:
            print("digital")

            Vendor_Name = data_extractor_alphanumeric(data,'Original',1,data_dict,'Invoice Number','Vendor_Name',l,'[A-Za-z]{4}\s+[A-Za-z]{5}\s+[A-Za-z]{10}\s+[A-Za-z]+\.\s+[A-Za-z]+\.',0)
            print("========Vendor_Name==============")
            print(Vendor_Name)
            

            Invoice_Number = data_extractor_alphanumeric(data,'Invoice Number',1,data_dict,'Shipping','Invoice_Number',l,'[A-Z]+[0-9]+\/[A-Z]+\/[0-9]+',0)
            print("========Invoice_Number==============")
            print(Invoice_Number)
            

            Invoice_Date = data_extractor_alphanumeric(data,'Invoice Number',1,data_dict,'Shipping','Invoice_Date',l,'[0-9]{2}\-[0-9]+\-[0-9]{4}',0)
            print("=======Invoice_Date=============")
            print(Invoice_Date)
            

            Po_Date ="NA"


            Po_Number = data_extractor_alphanumeric(data,"Invoice Number",1,data_dict,'Shipping','Po_Number',l,'\d{10}',0)
            print("**********Po_Number************")
            print(Po_Number)
            
            
            Lohia_Pan_Number =  data_extractor_alphanumeric(data,'Invoice Number',1,data_dict,'Shipping','Lohia_Pan_Number',l,'[A-Z]+[0-9]+[A-Z]',0)
            print("==========Lohia_Pan_Number==================")
            print(Lohia_Pan_Number)
            

            Gstin_Lohia =  data_extractor_alphanumeric(data,'Invoice Number',1,data_dict,'Shipping','Gstin_Lohia',l,'[0-9]{2}[A-Z]+[0-9]+[A-Z]+[0-9][A-Z]{2}',0)
            print("==========Gstin_Lohia=================")
            print(Gstin_Lohia)

            Gstin_client =  data_extractor_alphanumeric(data,'Original',1,data_dict,'Invoice Number','Gstin_client',l,'[0-9]+[A-Z]+[0-9]+[A-Z]+[0-9][A-Z]{2}',0)
            print("=======Gstin_client===================")
            print(Gstin_client)

            Grand_Total =  data_extractor_alphanumeric(data,'Invoice Total',1,data_dict,'\n','Vehicle No',l,'[0-9\,\.]+',-1)
            print("=======Grand_Total==========")       
            print(Grand_Total)

            Item_code = data_extractor_alphanumeric(data,'Discount',1,data_dict,'Freight','Item_code',l,'\d{10}',0)
            print("=======Item_code==========")
            print(Item_code)

            Vehicle_Number=data_extractor_alphanumeric(data,'Invoice Total',1,data_dict,'Representative','Vehicle No',l,'UP[A-Za-z0-9]+',-1)
            print("=======Vehicle_Number==========")
            print(Vehicle_Number)




            line_items = re.findall(r'[0-9\.\,]+\s+[0-9\.\,]+\s+[A-Z]+\s+[0-9\.\,]+\s+[0-9\.\,]+\s+[0-9\.\,]+|\d{6}\s+[0-9\.\,]+\s+[0-9\.\,]+\s+[0-9\.\,]+',data)
            line_items = "".join(line_items)
            line_items = line_items.split()
            print(len(line_items))
    ##        print(line_items)
            data_dict["Hsn_Sac_code"]=line_items[0]
            data_dict["Quantity"]=line_items[1]
            if len(line_items)==4:
                data_dict["Rate_per_unit"]="NA"
            else:
                data_dict["Rate_per_unit"]=line_items[-3]
            data_dict["Total_value"]=line_items[-1]
            
##            print("Hsn_Sac_code:",Hsn_Sac_code)
##            print("Quantity:",Quantity)
##            print("Rate_per_unit:",Rate_per_unit)
##            print("Total_value:",Total_value)
##            print("record inserted")

            conn = psycopg2.connect(database="postgres",user='postgres',password='Sameer@123',host='localhost',port= '5432')
        

            cursor = conn.cursor()

            query = "insert into spun_micro values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            value= (Vendor_Name,Invoice_Number,Invoice_Date,Po_Number,Po_Date,Lohia_Pan_Number,Gstin_client,Gstin_Lohia,Item_code,data_dict['Hsn_Sac_code'],data_dict['Quantity'],data_dict['Rate_per_unit'],data_dict["Total_value"],Grand_Total,Vehicle_Number)
            cursor.execute(query,value)
            conn.commit()
            print("record inserted")

        else:
            print("Scanned")
            def Trigger(input_path):
                output_path=r"C:\Python39\Lib\site-packages\Text"
                text=''
                os.chdir(output_path)
                main_call(input_path)
                
                text_all=''
                for file in os.listdir(r"C:\Python39\Lib\site-packages\Text"):
                    if file.endswith('text.txt'):
                        print(file)
                        text_=open(r"C:\Python39\Lib\site-packages\Text\\"+file,'r')
                        text_=text_.readlines()
                        text_=' '.join(text_)
                        text_all = text_all + text_


                for file in os.listdir(r"C:\Python39\Lib\site-packages\Text"):
                    os.remove(r"C:\Python39\Lib\site-packages\Text\\"+file)

                return text_all



            data = Trigger(path)
            data = ' '.join(data.split('\n'))
            # print(new_data)
            print('--------------')

            data_dict = {}
            l = ['(', ')', '.', '/', '-', ',', '%','&']
  
            ##    print(text)
            print('------'*10)

            
            Vendor_Name = data_extractor_alphanumeric(data,'Original',1,data_dict,'Order Number','Vendor_Name',l,'[A-Za-z]{4}\s+[A-Za-z]{5}\s+[A-Za-z]{10}\s+[A-Za-z]+\.\s+[A-Za-z]+\.',0)
            if Vendor_Name==0:
                Vendor_Name="Spun Micro Processing Pvt. Ltd."
            print("========Vendor_Name==============")
            print(Vendor_Name)


            Invoice_Number = data_extractor_alphanumeric(data,'Order Number',1,data_dict,'Delivery','Invoice_Number',l,'\w+\d+\/\w+\/\d+',0)
            print("========Invoice_Number==============")
            print(Invoice_Number)


            Invoice_Date = data_extractor_alphanumeric(data,'Order Number',1,data_dict,'Delivery','Invoice_Date',l,'[0-9]{2}\-[0-9]+\-[0-9]{4}',0)
            print("=======Invoice_Date=============")
            print(Invoice_Date)


            Po_Date ="NA"


            Po_Number = data_extractor_alphanumeric(data,"Order Number",1,data_dict,'Delivery','Po_Number',l,'\d{10}',0)
            print("**********Po_Number************")
            print(Po_Number)


            Lohia_Pan_Number =  data_extractor_alphanumeric(data,'Contact Number',1,data_dict,'Unit Price','Lohia_Pan_Number',l,'[A-Z]{5}\d{4}\w{1}',0)
            print("==========Lohia_Pan_Number==================")
            print(Lohia_Pan_Number)


            Gstin_Lohia =  data_extractor_alphanumeric(data,'Delivery',1,data_dict,'Shipping','Gstin_Lohia',l,'[0-9]{2}[A-Z]+[0-9]+[A-Z]+[0-9][A-Z]+',0)
            print("==========Gstin_Lohia=================")
            print(Gstin_Lohia)

            Gstin_client =  data_extractor_alphanumeric(data,'TAX INVOICE',1,data_dict,'Order Number','Gstin_client',l,'[0-9]{2}[A-Z]{5}[0-9]+[A-Z]+[0-9][A-Z]{2}',0)
            if Gstin_client!="09AABCS8714R1ZL":
                Gstin_client="09AABCS8714R1ZL"
            print("=======Gstin_client===================")
            print(Gstin_client)

            Grand_Total =  data_extractor_alphanumeric(data,'Invoice Total',1,data_dict,'Vehicle No',"Grand_Total",l,'[0-9\,\.]+',0)
            print("=======Grand_Total==========")       
            print(Grand_Total)
##             
            Item_code = data_extractor_alphanumeric(data,'Discount',1,data_dict,'Freight','Item_code',l,'\d{10}|\d{4}\s{2}\d{6}|\d{4}\s{2}\d{5}|\d{9}\s{2}\d{1}|\d{7}\s{2}\d{3}|\d{5}\s{2}\d{5}',0)
            print('Item_code:',Item_code)
##            
            if "  " in Item_code:
               Item_code=Item_code.replace("  ","")
            elif " " in Item_code:
               Item_code = data_extractor_alphanumeric(data,'Discount',1,data_dict,'Freight','Item_code',l,'\d{10}|\d{4}\s+\d{6}|\d{4}\s+\d{5}|\d{9}\s+\d{1}|\d{7}\s+\d{3}|\d{5}\s+\d{5}',1)
               Item_code=Item_code.replace(" ","")
    
    
    
            print("=======Item_code==========") 
            print(Item_code)
##
##
            Vehicle_Number=data_extractor_alphanumeric(data,'Invoice Total',1,data_dict,'Represent','Vehicle_Number',l,'UP[A-Za-z0-9]+',-1)
            if Vehicle_Number==0:
               Vehicle_Number=data_extractor_alphanumeric(data,'Invoice Total',1,data_dict,'ReprÃ©sentativ','Vehicle_Number',l,'UP[A-Za-z0-9]+',-1) 
            print("=======Vehicle_Number==========")
            print(Vehicle_Number)


            line_items = re.findall(r'[0-9\.\,]+\s+[0-9\.\,]+\s+[A-Z]+\s+[0-9\.\,]+\s+[0-9\.\,]+\s+[0-9\.\,]+|\d{6}\s+[0-9\.\,]+\s+[0-9\.\,]+\s+[0-9\.\,]+|[0-9\\.\,]+\s+[0-9\.\,]+\s+[A-Z]+\s+[0-9\.\,]+\s+[0-9\.\,]+',data)
            line_items = "".join(line_items)
            line_items = line_items.split()
            ##    print(len(line_items))
            ##        print(line_items)
            data_dict["Hsn_Sac_code"]=line_items[0]
            data_dict["Quantity"]=line_items[1]
            if len(line_items)==4:
                data_dict["Rate_per_unit"]="NA"
            elif len(line_items)==5:
                data_dict["Rate_per_unit"]=line_items[-2]
            else:
                data_dict["Rate_per_unit"]=line_items[-3]
            data_dict["Total_value"]=line_items[-1]
    

    ##                print("Hsn_Sac_code:",Hsn_Sac_code)
    ##                print("Quantity:",Quantity)
    ##                print("Rate_per_unit:",Rate_per_unit)
    ##                print("Total_value:",Total_value)
            conn = psycopg2.connect(database="postgres",user='postgres',password='Sameer@123',host='localhost',port= '5432')
        

            cursor = conn.cursor()

            query = "insert into spun_micro values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            value= (Vendor_Name,Invoice_Number,Invoice_Date,Po_Number,Po_Date,Lohia_Pan_Number,Gstin_client,Gstin_Lohia,Item_code,data_dict['Hsn_Sac_code'],data_dict['Quantity'],data_dict['Rate_per_unit'],data_dict["Total_value"],Grand_Total,Vehicle_Number)
            cursor.execute(query,value)
            conn.commit()
            print("record inserted")
                    
                    


for file in os.listdir(path):
    new_name=file.replace(" ","_")
    os.rename(r'C:\Users\Sameer\Downloads\spun\spun\lohia_vendor\\'+file,r'C:\Users\Sameer\Downloads\spun\spun\lohia_vendor\\'+new_name)
    file1=(r'C:\Users\Sameer\Downloads\spun\spun\lohia_vendor\\'+new_name)
    print(file1)
    read_file(file1)


