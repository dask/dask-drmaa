import drmaa
import os

def main():
   """
   Submit a job.
   Note, need file called sleeper.sh in current directory.
   """
   with drmaa.Session() as s:
       print('Creating job template')
       jt = s.createJobTemplate()
       jt.remoteCommand = os.path.join('/dask-drmaa', 'scratch', 'sleeper.sh')
       jt.args = ['42', 'Simon says:']
       jt.joinFiles=True
       jt.outputPath = ':/dask-drmaa/scratch/'
       jt.errorPath= ':/dask-drmaa/scratch/'

       jobid = s.runJob(jt)
       print('Your job has been submitted with ID %s' % jobid)

       print('Cleaning up')
       s.deleteJobTemplate(jt)

if __name__=='__main__':
   main()
