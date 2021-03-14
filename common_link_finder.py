'''
Module: Coomon link Finder
Description: finds number of href and anchor text on pages   
Created by: Asari Abdur Rahman
Created on: 13-3-21
'''

import argparse, json, csv, os, requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures

parser= argparse.ArgumentParser(description="Common link finder: Takes a file with links")

valid_links=[]


def verify_file(file_path):
    if os.path.isfile(file_path):
        if validate_links(file_path):
            return file_path
    else:
        raise argparse.ArgumentTypeError("Please pass valid file")

def validate_links(file_path):
    try:
        with open(file_path) as f:
            content= f.read()
            links= content.splitlines()
            for x in links:
                result = urlparse(x)
                if all([result.scheme, result.netloc, result.path]):
                    valid_links.append(x)
            print(f"Found {len(valid_links)} valid links out of {len(links)} in file")
        return True
    except Exception as x:
        raise argparse.ArgumentTypeError("Please pass valid file")


def start_process(file_name):
    dest_file= str(file_name.split(".").pop(0))+"_dest.csv"
    print(dest_file)
    with open(dest_file, 'w') as file:
        writer = csv.writer(file)
        writer.writerow(["href", "anchor_text", "total_occurence"])
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for link in valid_links:
            futures.append(
            executor.submit( scrape_page,dest_file , link)
            )
        '''for future in concurrent.futures.as_completed(futures):
            try:
                print(future.result())
            except requests.ConnectTimeout:
                print("ConnectTimeout.")'''
           

    count_anchors(dest_file)


def scrape_page(dest_file,link):
    try:
        content= requests.get(link)
        html=content.text
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a', href=True): 
            if a.text: 
                # links_with_text.append(a['href'])
                with open(dest_file, 'a') as file:
                    writer = csv.writer(file, lineterminator='\n')
                    writer.writerow([a['href'],a.text,1])
                # print(a['href'],a.text)
        return True
    except Exception as e:
        print(e)
        return e


def count_anchors(dest_file):
    df= pd.read_csv(dest_file)
    df2= df.groupby(['href','anchor_text'], as_index=False).agg({"total_occurence": "sum"})
    # print(df2.head(10))
    print(df2.sort_values(by=['total_occurence'],ascending=False))
    df2.to_csv("result_file.csv",index=False)



if __name__=="__main__":
    parser.add_argument('-f', '--file', type= verify_file, required=True, help="Pass file with links in new line in .txt or .csv")

    args= parser.parse_args()
    file_name = os.path.basename(args.file)
    start_process(file_name)



