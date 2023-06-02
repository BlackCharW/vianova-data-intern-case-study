import csv

def output_callable(result_file,**context):
    result = context['task_instance'].xcom_pull(task_ids='processor')
    
    with open(result_file, 'w', newline='') as tsv_file:
        writer = csv.writer(tsv_file, delimiter='\t')
        
        writer.writerow(['Country Code', 'Country Name'])
        
        for rs in result:
            writer.writerow([rs[0], rs[1]])

    print("Results saved to file: ", result_file)
        