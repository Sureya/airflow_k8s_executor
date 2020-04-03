# Airflow in Kubernetes Executor

### Pre-requisite
- Download Docker Desktop from [here](https://www.docker.com/products/docker-desktop] and setup in your local machine)
- Download Virtualbox from [here](https://www.virtualbox.org/wiki/Downloads) and setup in your local machine
- Install Minikube, Helm & add package repo
    ```bash
          brew update
  
          # install minikube
          brew install minikube
          
          # install helm
          brew install kubernetes-helm
          
          # add package repository to helm 
          helm repo add stable https://kubernetes-charts.storage.googleapis.com
    ``` 

## How do I Build ?

```bash
# ALL .PY Files in dags/ folder will be loaded as DAGS

$ make start 
# yup! that's it 
# the command takes few minutes to start, so trust the process & wait.

#Once the Make command is done execute the following commnds in different terminals
$ kubectl get pods --watch # to monitor the pod health
$ minikube dashboard  # to get the K8S dashboard

# If this URL doesn't load wait for few mins until the K8S dashboard becomes healthy. (usually takes 6-10 minutes)
$ minikube service airflow-web -n airflow # to load the Airflow UI page 

# Once you are done with the services you can stop all the services using following command 
$ make cleanup

```

### Example Image provided 

- I have included a sample countries.csv file
- The Idea is that we would create an example  DAG that would have following functionality 
    
    - Task1: Take input file & execute `apply_lower` step
        - Converts all Country name to lower case 
        - The output of this step is stored to S3 in `run_id/apply_lower/output.csv`  
    
    - Task2: Take output file from step 1 & execute `apply_code` step
        - For each country name available we calculate Country code using pycountry library
        - The output of this step is stored to S3 in `run_id/apply_code/output.csv`  
    
    - Task3: Take output file from step 2 & execute `apply_currency` step
        - For each country name available we make REST call and parse the currency code 
        - The output of this step is stored to S3 in `run_id/apply_currency/output.csv`  
    
_This is not provided as a DAG, the code & Dockerfile is given so that you can 
start focusing on creating a docker image and play with creating KubernetesPodOperator on your own for this application_

Steps to re-produce  
```bash
# To Build 
cd src 
docker build -t country . 

# To Run the application locally execute the following command 
$ docker run -v ~/.aws:/app/credentials -e S3_FILE_NAME="{BUCKET_NAME}/airflow-poc/run_001/input/countries.csv" -e TASK_NAME="apply_lower"  country
$ docker run -v ~/.aws:/app/credentials -e S3_FILE_NAME="{BUCKET_NAME}/airflow-poc/run_001/input/countries.csv" -e TASK_NAME="apply_code"  country
$ docker run -v ~/.aws:/app/credentials -e S3_FILE_NAME="{BUCKET_NAME}/airflow-poc/run_001/input/countries.csv" -e TASK_NAME="apply_currency"  country
```
