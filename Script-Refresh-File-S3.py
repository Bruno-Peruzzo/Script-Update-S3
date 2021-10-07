import paramiko
import os
import time
import boto
import boto.s3
import boto.s3.connection
import os.path
from pathlib import Path


ftp_server = "<IP DO SERVIDOR SFTP>"
port = 22
sftp_file = "<LOCAL/DO/ARQUIVO A SER BAIXADO NO SFTP>"
local_file = "<CAMINHO/PARA/ONDE SERA BAIXADO>"
ssh_conn = sftp_client = None
username = "<USUARIO>"
password = "<PASSWORD>"

start_time = time.time()

#For para realizar o dowload do arquivo, definindo o chunksize(tamanho do bloco de arquivo lido pela memoria) via parametro window_size
MAX_RETRIES = 1
for retry in range(MAX_RETRIES):
    try:
        ssh_conn = paramiko.Transport((ftp_server, port))
        ssh_conn.connect(username=username, password=password)
        window_size = pow(4, 12)#about ~16MB chunks
        max_packet_size = pow(4, 12)
        sftp_client = paramiko.SFTPClient.from_transport(ssh_conn, window_size=window_size, max_packet_size=max_packet_size)
        sftp_client.get(sftp_file, local_file)
        
    except (EOFError, paramiko.ssh_exception.SSHException, OSError) as x:
        retry += 1
        print("%s %s - > retrying %s..." % (type(x), x, retry))
        time.sleep(abs(retry) * 10)
        # back off in steps of 10, 20.. seconds 
    finally:
        if hasattr(sftp_client, "close") and callable(sftp_client.close):
            sftp_client.close()
        if hasattr(ssh_conn, "close") and callable(ssh_conn.close):
           ssh_conn.close()


print("Loading File %s Took %d seconds " % (sftp_file, time.time() - start_time))


#Renomear arquivo com a data atual d dia m mes Y year

f_path = "<LOCAL/DO/ARQUIVO A SER RENOMEADO>"
t = os.path.getctime(f_path)
t_str = time.ctime(t)
t_obj = time.strptime(t_str)

#Setar parametros do arquivo nome d dia m mes Y year
form_t = time.strftime("<NOME DO ARQUIVO>-%d-%m-%Y", t_obj)
os.rename(f_path, os.path.split(f_path)[0] + '/' + form_t + os.path.splitext(f_path)[1])

#Fazer upload para bucket S3

cmd = 'aws s3 cp <LOCAL/DO/ARQUIVO NA SUA MAQUINA> <URI DA BUCKET>
os.system(cmd)

#Deletar arquivo apos feito uploud para bucket.
for filename in Path("<LOCAL/DO/ARQUIVO>").glob("REL*.txt"):
    filename.unlink()

print ('Arquivo deletado')

exit()