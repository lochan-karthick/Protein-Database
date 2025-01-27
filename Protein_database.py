"""
Project Overview:
This script creates a SQLite database that allows us to organise and later derive key insights from 
Biological data, including subject data, transcript abundance, protein abundance, and metabolite abundance.
It also allows us to:
1. Create the database structure.
2. Populate the database with provided input files.
3. Perform various queries and a visualisation plot.
"""

#Importing the necessary libraries:

import sqlite3
import matplotlib.pyplot as plt
import csv

#Creating the Database Structure:

def database_struct(path_to_database):
    
    """
    Creates the SQLite database blueprint with the tables: Subjects, Samples,
    TranscriptAbundance, ProteinAbundance, MetaboliteAbundance, and Annotation.

    Args:
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    try: 
    #Establishing Connection To Sqllite Server and Creating Cursor Object:
    
        connection = sqlite3.connect(path_to_database)
        cursor = connection.cursor()
    
        print("Database Construction Has Begun")
    
    #Using the Cursor Object to Execute SQL Commands to Create the Necessary Tables:

        cursor.executescript("""
        CREATE TABLE Subjects(
            SubjectID TEXT PRIMARY KEY,
            Sex TEXT,
            Age INTEGER,
            BMI REAL,
            Race TEXT,
            SSPG REAL,
            InsulinStatus TEXT
        );
    
        CREATE TABLE Samples (
            SampleID TEXT PRIMARY KEY,
            SubjectID TEXT,
            VisitID INTEGER,
            FOREIGN KEY (SubjectID) REFERENCES Subjects(SubjectID)
        );
    
        CREATE TABLE TranscriptAbundance (
            SampleID TEXT,
            TranscriptID TEXT,
            Abundance REAL,
            PRIMARY KEY (SampleID, TranscriptID),
            FOREIGN KEY (SampleID) REFERENCES Samples(SampleID)
        );
    
        CREATE TABLE ProteinAbundance(
            SampleID TEXT,
            ProteinID TEXT,
            Abundance REAL,
            PRIMARY KEY(SampleID, ProteinID),
            FOREIGN KEY (SampleID) REFERENCES Samples(SampleID)
        );
    
        CREATE TABLE MetaboliteAbundance(
            SampleID TEXT,
            PeakID Text,
            Abundance REAL,
            PRIMARY KEY(SampleID, PeakID),
            FOREIGN KEY(SampleID) REFERENCES Samples(SampleID)
        );
    
        CREATE TABLE Annotation(
            PeakID TEXT,
            Metabolite TEXT,
            KEGG TEXT,
            HMDB TEXT,
            Pathway TEXT,
            PRIMARY KEY(PeakID, Metabolite)
        );
        """)
        
        connection.commit()
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        
    finally:
        connection.close()
        
#Parsing the Subjects File and Populating the Subjects Table:
    
def subject_table_populator(path_to_file, path_to_database):
    
    """
    Parses a CSV file and populates the Subjects table with data.

    Args:
        path_to_file (str): Path to the Subjects CSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Subjects")
    
    with open(path_to_file, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        
        # Iterating over the rows and inserting records into the database
        
        for row in reader:
            
            #Null Value Handling because of Errors in Running Queries
            age = None if row['Age'] in ('NA','') else row['Age']
            
            cursor.execute('''
                INSERT INTO Subjects(SubjectID, Sex, Age, BMI, Race, SSPG, InsulinStatus)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['SubjectID'], 
                row['Sex'], 
                row['Age'], 
                row['BMI'], 
                row['Race'], 
                row['SSPG'], 
                row['IR_IS_classification']
            ))
            
    connection.commit()
    connection.close()
    print("Subjects table populated successfully.")
            
#Extracting the Visit ID and Using it to Create a Samples Table:

def sample_table_populator(path_to_file, path_to_database):
    
    """
    Parses a TSV file and populates the Samples table by splitting SampleID into
    SubjectID and VisitID.

    Args:
        path_to_file (str): Path to the Samples TSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Samples")
    
    #Iterating Over the File and Populating the Table
    
    with open(path_to_file, 'r', encoding='utf-8-sig') as file:
        
        reader = csv.DictReader(file, delimiter = '\t')
        reader.fieldnames = [header.strip() for header in reader.fieldnames]
        
        for row in reader:
            try:
                
                sample_id = row['SampleID']
                
                #Splitting the SampleID column into Subject ID and Visit ID and Inserting into Table
                
                subject_id, visit_id = sample_id.split('-')
                cursor.execute('''
                    INSERT OR IGNORE INTO Samples(SampleID, SubjectID, VisitID)
                    VALUES(?, ?, ?)
                ''', (sample_id, subject_id, visit_id))
                
            except KeyError:
                
                print(f"SampleID not found: {row}")
                continue
                    
    connection.commit()
    connection.close()
    print("Samples table populated successfully.")
    
#Parsing the Transcriptome Abundance File and Populating the Table:

def transcriptome_populator(path_to_file, path_to_database):
    
    """
    Populates the TranscriptAbundance table with data from a TSV file.

    Args:
        path_to_file (str): Path to the Transcript Abundance TSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM TranscriptAbundance")
    
    #Iterating Over the File and Populating the Table
    
    with open(path_to_file, 'r') as file:
        reader = csv.DictReader(file, delimiter = '\t')
        for row in reader:
            
            #Popping the Sample_ID so That Only Transcriptome Data is Stored
            
            sample_id = row.pop('SampleID')
            for transcript_id, abundance in row.items():
                cursor.execute('''
                    INSERT INTO TranscriptAbundance (SampleID, TranscriptID, Abundance)
                    VALUES (?, ?, ?)
                ''', (sample_id, transcript_id, abundance))
                
    connection.commit()
    connection.close()
    print("Transcript Abundance Table Populated Successfully.")
    
#Parsing the Protein Abundance File and Populating the Table:

def protein_populator(path_to_file, path_to_database):
    
    """
    Populates the ProteinAbundance table with data from a TSV file.

    Args:
        path_to_file (str): Path to the Protein Abundance TSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """

    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM ProteinAbundance")
    
    #Iterating Over the File and Populating the Table
    
    with open(path_to_file, 'r') as file:
        reader = csv.DictReader(file, delimiter = '\t')
        for row in reader:
            
            #Popping the Sample_ID so That Only Protein Data is Stored
            
            sample_id = row.pop('SampleID')
            for protein_id, abundance in row.items():
                cursor.execute('''
                    INSERT INTO ProteinAbundance (SampleID, ProteinID, Abundance)
                    VALUES (?, ?, ?)
                ''', (sample_id, protein_id, abundance))
                
    connection.commit()
    connection.close()
    print("Protein Abundance Table Populated Successfully.")
    
#Parsing the Metabolite Abundance File and Populating the Table:

def Metabolite_populator(path_to_file, path_to_database):
    
    """
    Populates the MetaboliteAbundance table with data from a TSV file.

    Args:
        path_to_file (str): Path to the Metabolite Abundance TSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM MetaboliteAbundance")
    
    #Iterating Over the File and Populating the Table
    
    with open(path_to_file, 'r') as file:
        reader = csv.DictReader(file, delimiter = '\t')
        for row in reader:
            
            #Popping the Sample_ID so That Only Metabolite Data is Stored
            
            sample_id = row.pop('SampleID')
            for peak_id, abundance in row.items():
                cursor.execute('''
                    INSERT INTO MetaboliteAbundance (SampleID, PeakID, Abundance)
                    VALUES (?, ?, ?)
                ''', (sample_id, peak_id, abundance))
                
    connection.commit()
    connection.close()
    print("Metabolite Table Populated Successfully.")
    
#Parsing the Annotations File and Populating the Table:

def Annotations_populator(path_to_file, path_to_database):
    
    """
    Populates the Metabolite Annotations table with data from a TSV file.

    Args:
        path_to_file (str): Path to the Metabolite Annotations TSV file.
        path_to_database (str): Path to the SQLite database file.

    Returns:
        None
    """
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    cursor.execute("DELETE FROM Annotation")
    
    #Iterating Over the File and Populating the Table
    
    with open(path_to_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute('''
                INSERT INTO Annotation (PeakID, Metabolite, KEGG, HMDB, Pathway)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['PeakID'], row['Metabolite'], row['KEGG'], row['HMDB'], row['Pathway']))
            
    connection.commit()
    connection.close()
    print("Metabolite Annotation Table Populated successfully.")
    
#Query 1- Retrieve SubjectID and Age of subjects whose age is greater than 70:

def subjects_over_70(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("SELECT SubjectID, Age FROM Subjects WHERE Age > 70")
    answer = cursor.fetchall()
    
    connection.close()
    
    if answer:
        return answer
    else:
        print("No subjects over age 70.")
        return[]

#Query 2- Retrieve all female SubjectID who have a healthy BMI (18.5 to 24.9). Sort the results in descending order:

def females_with_healthy_BMI(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT SubjectID FROM Subjects
        WHERE Sex = 'F' and BMI BETWEEN 18.5 and 24.9
        ORDER BY SubjectID DESC
        """)
    answer = cursor.fetchall()
    
    connection.close()
    
    if answer:
        return answer
    else:
        print("No Females with Healthy BMI.")
        return []
    
#Query 3- Retrieve the Visit IDs of Subject 'ZNQOVZV'. 

def visits_by_ZNQOVZV(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT VisitID 
        FROM Samples
        WHERE SubjectID = 'ZNQOVZV';
    """)
    answer = cursor.fetchall()
    
    connection.close()
    return answer

#Query 4- Retrieve distinct SubjectIDs who have metabolomics samples and are insulin-resistant.

def subjects_metabolomic_insulin_resistant(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT DISTINCT S.SubjectID
        FROM Samples AS S
        JOIN Subjects AS Sub ON S.SubjectID = Sub.SubjectID
        WHERE Sub.InsulinStatus = 'IR'
    """)
    answer = cursor.fetchall()
    
    connection.close()
    
    if answer:
        return answer
    else:
        print("No Insulin-Resistant Subjects with metabolomics samples found")
        return []
    
#Query 5- Retrieve the unique KEGG IDs that have been annotated for the following peaks: 'nHILIC_121.0505_3.5', 'nHILIC_130.0872_6.3', 'nHILIC_133.0506_2.3', 'nHILIC_133.0506_4.4'.

def KEGG_id_retriever(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT DISTINCT KEGG FROM Annotation
        WHERE PeakID IN (
            'nHILIC_121.0505_3.5',
            'nHILIC_130.0872_6.3',
            'nHILIC_133.0506_2.3',
            'nHILIC_133.0506_4.4'
        )
    """)
    answer = cursor.fetchall()
    
    connection.close()
    
    if answer:
        return answer
    else:
        print("No KEGG IDs found for specified peaks")
        return []
    
#Query 6- Retrieve the minimum, maximum and average age of Subjects.

def statistical_data_on_age(path_to_database):
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    try:
        cursor.execute("""
            SELECT
                MIN(Age) AS MinAge, 
                MAX(Age) AS MaxAge, 
                AVG(Age) AS AvgAge
            FROM Subjects
            WHERE Age IS NOT NULL AND Age != 'NA';
        """)
        
        answer = cursor.fetchone()  

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    
    finally:
        connection.close()
    
    # Handling Null values
    if not answer or all(value is None for value in answer):
        print("No valid data found.")  
    
    #Returning List to Iterate Over
    return [answer]  

    
#Query 7- Retrieve the list of pathways from the annotation data, and the count of how many times each pathway has been annotated.

def pathway_annotation_counter(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT Pathway, COUNT(*) AS AnnotationCount
        FROM Annotation
        GROUP BY Pathway
        HAVING AnnotationCount >= 10
        ORDER BY AnnotationCount DESC
    """)
    answer = cursor.fetchall()
    
    connection.close()
    
    if answer:
        return answer
    else:
        print("No pathways with at least 10 annotations found.")
        return []

#Query 8- Retrieve the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' across all samples.

def max_A1BG_abundance(path_to_database):
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT MAX(Abundance)
        FROM TranscriptAbundance
        WHERE TranscriptID = 'A1BG' 
        AND SampleID IN (
            SELECT SampleID 
            FROM Samples 
            WHERE SubjectID = 'ZOZOW1T'
        )
    """)
    answer = cursor.fetchone()
    connection.close()

    if answer and answer[0] is not None:
        return answer[0]
    else:
        print("No data found for A1BG abundance for Subject 'ZOZOW1T'.")
        return None

#Query 9- Retrieve the subjects’ age and BMI.

def age_bmi_plot(path_to_database):
    
    connection = sqlite3.connect(path_to_database)
    cursor = connection.cursor()
    
    cursor.execute("""
        SELECT Age, BMI
        FROM Subjects
        WHERE Age IS NOT NULL AND BMI IS NOT NULL
    """)
    data = cursor.fetchall()
    
    connection.close()
    
    # Debugging output
    print(f"Retrieved data: {data}")
    
    # Ensure all values are numeric and filter out invalid rows
    numeric_data = [(float(age), float(bmi)) for age, bmi in data if isinstance(age, (int, float)) and isinstance(bmi, (int, float))]
    
    # Separate ages and bmis
    if numeric_data:
        ages, bmis = zip(*numeric_data)  
    else:
        ages, bmis = [], []
    
    # Plot the data
    if ages and bmis:
        plt.scatter(ages, bmis)
        plt.xlabel("Age")
        plt.ylabel("BMI")
        plt.title("Age vs BMI Plot")
        plt.savefig("age_vs_bmi_scatterplot.png")
        print("Plot saved as 'age_vs_bmi_scatterplot.png'")
    else:
        print("No valid Age and BMI data to plot.")
    
    return data

"""
Command-Line:
This script allows us to interact with it through the command line. 
1. --createdb: Creates the database blueprint.
2. --loaddb: Loads data into the database from input files.
3. --querydb: Runs specific queries on the database.
Queries include:
- Query 1: Retrieves SubjectID and Age of subjects older than 70.
- Query 2: Retrieves all female SubjectIDs with healthy BMI.
- Query 3: Retrieves VisitIDs for a specific subject.
- Query 4: Retrieves distinct SubjectIDs with metabolomics samples.
- Query 5: Retrieves KEGG IDs for specific peaks.
- Query 6: Retrieves statistical age data (min, max, average).
- Query 7: Retrieves pathway annotations with counts >= 10.
- Query 8: Retrieves maximum abundance of transcript A1BG.
- Query 9: Generates an Age vs BMI scatter plot.
"""

def main():
    
    #Taking User Input from Commandline using Argparse Library:
    
    import argparse
    
    #Initializing Possible Input from Command Line:
    
    parser = argparse.ArgumentParser(description="Database query executor.")
    parser.add_argument("--createdb", action="store_true", help="Create the database schema.")
    parser.add_argument("--loaddb", action="store_true", help="Load data into the database.")
    parser.add_argument("--querydb", type=int, help="Run a specific query on the database.")
    parser.add_argument("database", help="Path to the SQLite database file.")
    parser.add_argument("--subjects", type=str, help="Path to Subjects CSV file.")
    parser.add_argument("--samples", type=str, help="Path to Samples CSV file.")
    parser.add_argument("--transcripts", type=str, help="Path to Transcript Abundance TSV file.")
    parser.add_argument("--proteome", type=str, help="Path to Proteome Abundance TSV file.")
    parser.add_argument("--metabolome", type=str, help="Path to Metabolome Abundance TSV file.")
    parser.add_argument("--annotations", type=str, help="Path to Metabolome Annotation CSV file.")
    args = parser.parse_args()
    
    #Using if Statements to Match Specific User Input to Code Function:
    if args.createdb:
        print("Creating Database Structure")
        database_struct(args.database)
        print("Database Structure has Been Created")
        
    if args.loaddb:
        if args.subjects:
            subject_table_populator(args.subjects, args.database)
        if args.samples:
            sample_table_populator(args.samples, args.database)
        if args.transcripts:
            transcriptome_populator(args.transcripts, args.database)
        if args.proteome:
            protein_populator(args.proteome, args.database)
        if args.metabolome:
            Metabolite_populator(args.metabolome, args.database)
        if args.annotations:
            Annotations_populator(args.annotations, args.database)
        print("All data loaded successfully.")
        
    #Using if Statements to Match Specific User Input to the respective Query:
    if args.querydb:
        if args.querydb == 1:
            answer = subjects_over_70(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 2:
            answer = females_with_healthy_BMI(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 3:
            answer = visits_by_ZNQOVZV(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 4:
            answer = subjects_metabolomic_insulin_resistant(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 5:
            answer = KEGG_id_retriever(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 6:
            answer = statistical_data_on_age(args.database)
            if answer:
                min_age, max_age, avg_age = answer[0]
                print(f"Minimum Age: {min_age:.2f}")
                print(f"Maximum Age: {max_age:.2f}")
                print(f"Average Age: {avg_age:.2f}")
            else:
                print("No valid age data")
            
                
        elif args.querydb == 7:
            answer = pathway_annotation_counter(args.database)
            for row in answer:
                print("\t".join(map(str,row)))
                
        elif args.querydb == 8:
            answer = max_A1BG_abundance(args.database)
            print(f"Maximum Abundance of A1BG: {answer}")
            
        elif args.querydb == 9:
            answer = age_bmi_plot(args.database)
            if answer:
                print("Age vs BMI data successfully retrieved and plotted.")
            else:
                print("No valid Age and BMI data available to plot.")
        else:
            print("Invalid query number. Please choose between 1 and 9.")
            return
        

if __name__ == "__main__":
    main() 