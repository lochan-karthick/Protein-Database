# Protein-Database

This Python-based SQLite database stores, queries, and visualizes biological data, including proteins, metabolites, and transcript abundance.

#Running the script:

This will create the database structure:

python3 Protein_database.py create_db "Name of database(up to you)".sqlite

This will load the data and populate all the tables:

python3 Protein_database.py --loaddb "Name of database(up to you)".sqlite \
    --subjects data/subjects.csv \
    --samples data/samples.csv \
    --transcripts data/transcripts.tsv \
    --proteome data/proteins.tsv \
    --metabolome data/metabolome.tsv \
    --annotations data/annotations.csv

#To run queries:

1.Retrieve SubjectID and Age of subjects whose age is greater than 70.

2.Retrieve all female SubjectID who have a healthy BMI (18.5 to 24.9) in descending order.

3.Retrieve the Visit IDs of Subject 'ZNQOVZV'. 

4.Retrieve distinct SubjectIDs who have metabolomics samples and are insulin-resistant.

5.Retrieve the unique KEGG IDs that have been annotated for the following peaks: 
  a.'nHILIC_121.0505_3.5'
  b.'nHILIC_130.0872_6.3'
  c.'nHILIC_133.0506_2.3'
  d.'nHILIC_133.0506_4.4'
  
6.Retrieve the minimum, maximum and average age of Subjects.

7.Retrieve the list of pathways from the annotation data, and the count of how many times each pathway has been annotated (count>=10)

8.Retrieve the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' across all samples.

9.Retrieve the subjects’ age and BMI and Visualise using a scatter plot using the library matplotlib.

#Command Line Script:
python3 Protein_database.py --querydb (any number from 1 to 9 (based off above list)) (Name of database).sqlite



