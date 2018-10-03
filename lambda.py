from datetime import datetime
from datetime import timedelta
import time
import sys
import boto3
import os
import json



## Necessário criar as variaveis de ambiente abaixo para funcionamento da lambda
bucket = os.environ['s3_bucket']
region = os.environ['region']
days = os.environ['days_retention'] # Delta de dias. Exemplo: days=7 deleta somente snaphosts com data de criação maiores que 7.

# seta o client do boto3
ec2 = boto3.setup_default_session(region_name=region)
ec2 = boto3.client('ec2', region_name=region)

# pega o numero da conta aws
myAccount = boto3.client('sts').get_caller_identity().get('Account')




def send_s3(snap_list,count,total_size):
    s3_service = boto3.resource('s3')

    # Cria log de arquivo para upload no S3
    result = {"SnapshotsNumber": count, "TotalSize": total_size,"Snapshots": snap_list}
    result_json = json.dumps(result, indent=4)
    print(result_json)
    pathfile_add_region = '/tmp/' + region
    pathfile = pathfile_add_region+'_snapshot.txt'
    arquivo = open(pathfile, 'w')
    arquivo.write(str(result_json))
    arquivo.close()

    #Upload para o S3
    today = datetime.now().strftime("%Y%m%d%H%M")

    s3_file = region + '_snapshot'+str(today)+'.txt'
    s3_service.Bucket(bucket).upload_file(pathfile, s3_file)


def remove_snapshots():
    delete_time = datetime.today() - timedelta(days=int(days))

    # Retorna todas as snapshots da conta
    snapshots = ec2.describe_snapshots(MaxResults=1000, OwnerIds=[myAccount]).get('Snapshots')


    deletion_counter = 0
    size_counter = 0
    snap_list = []

    # Verifica qual o tempo de criação da snapshot e deleta a mesma conforme o delta informado.
    for snapshot in snapshots:
        start_time = datetime.strptime(str(snapshot['StartTime']), '%Y-%m-%d %H:%M:%S+00:00')
        print(snapshot['SnapshotId'])
        if start_time < delete_time:
            print("Deleting {id}, size {size} GB.".format(id=snapshot['SnapshotId'],size=snapshot['VolumeSize']))
            
            deletion_counter = deletion_counter + 1
            size_counter = size_counter + snapshot['VolumeSize']
            volume_id = snapshot['VolumeId']
            try:
                volume = ec2.describe_volumes(VolumeIds=[volume_id]).get('Volumes', [])
                if 'Iops' is volume.keys():
                    iops = volume['Iops']
                else:
                    iops = None
                volume_type = volume['VolumeType']
            except Exception:
                print("VolumeId not found.")
                iops = None
                volume_type = None
            
            snap_data = {"Id": snapshot['SnapshotId'], "Size": str(snapshot['VolumeSize'])+"G", "StartTime": str(start_time), "VolumeId": volume_id, "VolumeType": volume_type, "Iops": iops}
            snap_list.append(snap_data)
            # Habilita o recurso de manipulação, permitindo a exclusão do snapshot
            ec = boto3.resource('ec2', region_name=region)
            snap = ec.Snapshot(id=snapshot['SnapshotId'])
            try:
                snap.delete(DryRun=True) #Necessário estar com a opção =False para de fato deletar.
            except Exception:
                print("Error in deleting {id}".format(id=snapshot['SnapshotId']))
                snap_list.pop()
                deletion_counter = deletion_counter - 1
                size_counter = size_counter - snapshot['VolumeSize']
                
            
    
    send_s3(snap_list,deletion_counter,size_counter)
    print("Deleted {number} snapshots totalling {size} GB".format(
    	number=deletion_counter,
    	size=size_counter
    ))

def lambda_handler(event, context):
    remove_snapshots()
    return 'successful' 

