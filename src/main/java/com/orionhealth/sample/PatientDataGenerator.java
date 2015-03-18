package com.orionhealth.sample;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.joda.time.DateTime;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * This class is a driver class that will connect to a Cassandra cluster, create
 * a keyspace, some column families within the keyspace and then load those CFs
 * with some random patient data.<br>
 * <br>
 * Generally speaking, for each patient a random number of encounters are
 * created over a period of time and for each of those encounters a random
 * number of lab results are created. A certain percentage of those results will
 * be considered abnormal, according to generally accepted ranges for the lab
 * tests.<br>
 * <br>
 * Because of this taxonomy, the user should be aware that when generating data
 * for a large number of patients that the number of results can grow quite
 * large. As an example, for 100K patients, up to 500K encounters could be
 * created and 2.5M results could be created.<br>
 * <br>
 * This class is by no means a showcase of how to connect to and use a Cassandra
 * cluster, but rather a quick and dirty way to load a keyspace with test data
 * for other purposes. Therefore, it doesn't handle any types of errors and
 * assumes that the cluster is on your local box.<br>
 * <br>
 * Your mileage may vary according to use.
 * 
 * @author Jason Foster
 * 
 */
public class PatientDataGenerator {
    private static final String KEYSPACE_NAME = "ads";
    private static final int NUMBER_OF_PATIENTS = 1000;
    private static final double PERCENT_ABNORMAL_RESULT = .33;
    private static final int MEASUREMENT_PERIOD_YEAR = 2014;

    // For a simple page that gives zip code ranges, use the following link
    // http://www.empyrean.net/zipcodes.htm
    private static final boolean USE_ZIP_RANGE = true;
    private ZipRange zipRange = ZipRange.CT;
    
    private Date loadDate = new Date();
    private Cluster cluster;
    private Session session;
    private DataGenerator generator = new DataGenerator(NUMBER_OF_PATIENTS, 16);
    private DateTime minBirthDate = new DateTime(1915, 1, 1, 0, 0, 0, 000);
    private DateTime maxBirthDate = new DateTime(1999, 12, 31, 23, 59, 59, 999);
    private EncounterDataStrategy diabetesStrategy = null;
    private EncounterDataStrategy ischemiaStrategy = null;
    private EncounterDataStrategy careStrategy = null;
    private EncounterDataStrategy prevStrategy = null;
    

    private void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());

        }

        // Create and initialize a new session on the cluster
        session = cluster.connect();
        
        // Now that we have a session we can create our data generation strategies
        diabetesStrategy = new DiabetesDataStrategy(
                KEYSPACE_NAME, session, generator, PERCENT_ABNORMAL_RESULT, loadDate);
        ischemiaStrategy = new IVDDataStrategy(
                KEYSPACE_NAME, session, generator, PERCENT_ABNORMAL_RESULT, loadDate);
        careStrategy = new CareDataStrategy(
                KEYSPACE_NAME, session, generator, PERCENT_ABNORMAL_RESULT, loadDate);
        prevStrategy = new PrevDataStrategy(
                KEYSPACE_NAME, session, generator, PERCENT_ABNORMAL_RESULT, loadDate);
    }

    @SuppressWarnings("unused")
    private void createSchema() {
        if (createKeyspace()) {
            createColumnFamilies();
        }
    }

    private boolean createKeyspace() {
        if (cluster.getMetadata().getKeyspace(KEYSPACE_NAME) != null) {
            return false;
        }

        StringBuffer ddl = new StringBuffer();
        ddl.append("CREATE KEYSPACE ").append(KEYSPACE_NAME)
                .append(" WITH REPLICATION = ")
                .append("{'class':'SimpleStrategy', 'replication_factor':3};");

        session.execute(ddl.toString());
        return true;
    }

    private void createColumnFamilies() {
        StringBuffer ddl = new StringBuffer();

        // Patient table which holds basic demographics and social information
        ddl.append("create table ").append(KEYSPACE_NAME).append(".patient (")
                .append("patient_id int,").append("patient_name varchar,")
                .append("age int,").append("gender varchar,")
                .append("payor varchar,").append("zip_code varchar,")
                .append("aspirin_use boolean,").append("tobacco_use boolean,")
                .append("primary key (patient_id));");
        session.execute(ddl.toString());

        // Patient results table which holds results of lab tests
        ddl.setLength(0);
        ddl.append("create table ")
                .append(KEYSPACE_NAME)
                .append(".pat_results (")
                .append("patient_id int,")
                .append("test_name varchar,")
                .append("test_date timestamp,")
                .append("test_value double, ")
                .append("primary key (patient_id, test_name, test_date)) ")
                .append("with clustering order by (test_name asc, test_date desc);");
        session.execute(ddl.toString());

        // Encounters table which holds some rudimentary encounter data
        ddl.setLength(0);
        ddl.append("create table ").append(KEYSPACE_NAME)
                .append(".pat_encounters (").append("patient_id int,")
                .append("admission_date timestamp,")
                .append("encounter_id int,").append("admission_type text,")
                .append("discharge_date timestamp, ")
                .append("length_of_stay int,")
                .append("primary key (patient_id, admission_date)) ")
                .append("with clustering order by (admission_date desc);");
        session.execute(ddl.toString());

        // Patient readmission risk table
        ddl.setLength(0);
        ddl.append("create table ").append(KEYSPACE_NAME)
                .append(".pat_readmission_risk (").append("patient_id int,")
                .append("score int,").append("primary key (patient_id))");
        session.execute(ddl.toString());

        // Readmission risk stratification table
        ddl.setLength(0);
        ddl.append("create table ").append(KEYSPACE_NAME)
                .append(".readmission_stratification (").append("score int,")
                .append("count int,").append("primary key (score))");
        session.execute(ddl.toString());

        // Metric table for calculating various metrics
        ddl.setLength(0);
        ddl.append("create table ").append(KEYSPACE_NAME).append(".metrics (")
                .append("moniker text,").append("value int,")
                .append("primary key (moniker))");
        session.execute(ddl.toString());

        // Create all secondary indexes to support queries
        session.execute("create index patient_gender on " + KEYSPACE_NAME
                + ".patient(gender);");
        session.execute("create index patient_zip on " + KEYSPACE_NAME
                + ".patient(zip_code);");
        session.execute("create index patient_aspirin on " + KEYSPACE_NAME
                + ".patient(aspirin_use);");
        session.execute("create index patient_tobacco on " + KEYSPACE_NAME
                + ".patient(tobacco_use);");
        session.execute("create index pat_result_value on " + KEYSPACE_NAME
                + ".pat_results(test_value);");
        session.execute("create index encounter_id on " + KEYSPACE_NAME
                + ".pat_encounters(encounter_id);");
        session.execute("create index admission_type on " + KEYSPACE_NAME
                + ".pat_encounters(admission_type);");
        session.execute("create index readmission_score on " + KEYSPACE_NAME
                + ".pat_readmission_risk(score);");
    }

    private void loadData() {       
        // Load patient data
        System.out.print("Loading patients data....");
        try {
            for (int i = 1; i <= NUMBER_OF_PATIENTS; i++) {
                // Determine a random OHA id
                String ohaId = generator.generateRandomUUID();
                
                // Determine a gender for our patient
                String gender = generator.generateRandomGender();
    
                // Get a full name for our patient based on that gender
                String fullName = generator.generateRandomName(gender
                        .equalsIgnoreCase("M"));
    
                // Get a patient id to use for all our related data
                int patientId = generator.generateRandomIdentifier();
                String hicn = generator.generateRandomHICN();
                Date dob = generator.generateRandomTimestamp(minBirthDate.toDate(),
                        maxBirthDate.toDate());
    
                // Do a base insert into the patient/demographics table as well as a
                // mapping entry to generate a relationship between the patient and 
                // an hicn
                session.execute(generateInsertPatientDDL(patientId));
                session.execute(generateHicnXrefDML(hicn, ohaId));
                session.execute(generateMrnXrefDML(String.valueOf(patientId), ohaId));
                session.execute(generateBeneficiaryAssignmentDDL(hicn, gender,
                        fullName, dob));
                session.execute(generateBeneficiaryRankingDDL(hicn, gender,
                        fullName, dob));
                
                // We utilize different strategies for generating data for each
                // patient based on whether we want the patient to be a part of
                // a specific measure.  To decide which way the patient will go
                // we use modular arithmetic
                switch ((byte)(System.currentTimeMillis() % 4)) {
                case 0:
                    diabetesStrategy.generateEncounterData(patientId, MEASUREMENT_PERIOD_YEAR, 
                            gender.equalsIgnoreCase("M"));
                case 1:
                    ischemiaStrategy.generateEncounterData(patientId, MEASUREMENT_PERIOD_YEAR, 
                            gender.equalsIgnoreCase("M"));
                case 2:
                    careStrategy.generateEncounterData(patientId, MEASUREMENT_PERIOD_YEAR, 
                            gender.equalsIgnoreCase("M"));
                case 3:
                    ((PrevDataStrategy)prevStrategy).generateEncounterData(patientId, hicn, MEASUREMENT_PERIOD_YEAR, 
                            gender.equalsIgnoreCase("M"), fullName, dob);
                }
            }
        } catch (NoHostAvailableException nhaex) {
            Map<InetSocketAddress,Throwable> errors = nhaex.getErrors();
            System.out.println("Errors from exception\n" + errors.toString());
            nhaex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        System.out.println("complete!");
     }

    private String generateInsertPatientDDL(int patientId) {
        // One note here is that while we can generate patients who are dead
        // we don't, so we set death_indicator = 0 for all patients and do
        // not include a date_of_death for anyone.
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        StringBuffer sb = new StringBuffer(256);
        String gender = generator.generateRandomGender();
        ZipData data = USE_ZIP_RANGE ? 
                generator.generateRandomZipData(zipRange.getLowerBound(), zipRange.getUpperBound()) : 
                    generator.generateRandomZipData();
        sb.append("insert into ")
                .append(KEYSPACE_NAME)
                .append(".patient_demographics (patient_id,patient_id_src,city,state_or_province,date_of_birth,death_indicator,gender,zip_code,load_date) values (")
                .append(String.format("'%s','%s','%s','%s','%s','%s','%s','%s','%s'", patientId, "OHCP",
                        data.getCity(), data.getState(), 
                        sdf.format(generator.generateRandomTimestamp(
                                minBirthDate.toDate(), maxBirthDate.toDate())),
                        "0", gender, data.getZipCode(), sdf.format(loadDate)))
                .append(");");

        return sb.toString();
    }
    
    private String generateHicnXrefDML(String hicn, String ohaId) {
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
            .append(KEYSPACE_NAME)
            .append(".ads_patient_xref (uid, src, src_patientid, oha_patientid) values (")
            .append(String.format("%s, '%s','%s', '%s'", "uuid()", "CMS", hicn, ohaId))
            .append(");");

        return sb.toString();
    }
    
    private String generateMrnXrefDML(String mrn, String ohaId) {
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
            .append(KEYSPACE_NAME)
            .append(".ads_patient_xref (uid, src, src_patientid, oha_patientid) values (")
            .append(String.format("%s, '%s', '%s', '%s'", "uuid()", "OHCP", mrn, ohaId))
            .append(");");

        return sb.toString();
    }

    
    private String generateBeneficiaryAssignmentDDL(String hicn, String gender,
            String fullName, Date dob) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        String[] nameParts = fullName.split(",");
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
                .append(KEYSPACE_NAME)
                .append(".aco_beneficiary_assignment (uid,hicno,aco_participant_tin,dob,count_of_primary_care_services,load_date,deceased_bene_flag,firstname,lastname,gender,assignment_step_flag) values (")
                .append(String.format(
                        "%s,'%s','%s','%s',%s,'%s',%s,'%s','%s','%s',%s",
                        "uuid()", hicn, generator.generateRandomTIN(),
                        sdf.format(dob), generator.generateRandomCount(50),
                        sdf.format(new Date()),
                        (generator.generateRandomBoolean() ? 1 : 0),
                        nameParts[1].trim(), nameParts[0].trim(), gender,
                        (generator.generateRandomBoolean() ? 1 : 0)))
                .append(");");

        return sb.toString();
    }

    private String generateBeneficiaryRankingDDL(String hicn, String gender,
            String fullName, Date dob) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        String[] nameParts = fullName.split(",");

        // Just a note here on the use of these hoppers. When the data generator
        // gets created we tell it to create 16 of them so we can use them here.
        // Basically they are a randomly permuted array of integers up to the
        // number of patients we want, so they are ideal for randomly generating
        // a ranking from 1 to n.
        StringBuffer sb = new StringBuffer(256);
        sb.append("insert into ")
                .append(KEYSPACE_NAME)
                .append(".aco_patient_ranking (hicno,pat_first_name,pat_last_name,gender,dob,provider_npi1,provider_npi2,provider_npi3,clinic_identifier,caremedcon_rank,carefalls_rank,cad_rank,dm_rank,hf_rank,htn_rank,ivd_rank,pcmammogram_rank,pccolorectal_rank,pcflushot_rank,pcpneumoshot_rank,pcbmiscreen_rank,pctobaccouse_rank,pcbloodpressure_rank,pcdepression_rank) values(")
                .append(String.format("'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    hicn, nameParts[1].trim(), nameParts[0].trim(),
                    gender, sdf.format(dob),
                    generator.generateRandomTIN(), generator.generateRandomTIN(),
                    generator.generateRandomTIN(), generator.generateRandomTIN(),
                    generator.generateRandomRanking(0), generator.generateRandomRanking(1),
                    generator.generateRandomRanking(2), generator.generateRandomRanking(3),
                    generator.generateRandomRanking(4), generator.generateRandomRanking(5),
                    generator.generateRandomRanking(6), generator.generateRandomRanking(7),
                    generator.generateRandomRanking(8), generator.generateRandomRanking(9),
                    generator.generateRandomRanking(10), generator.generateRandomRanking(11),
                    generator.generateRandomRanking(12), generator.generateRandomRanking(13),
                    generator.generateRandomRanking(14), generator.generateRandomRanking(15)))
                .append(");");

        return sb.toString();
    }

    private void close() {
        session.close();
        cluster.close();
    }

    public static void main(String[] args) {
        String cassandraHost = "localhost";
        //String cassandraHost = "ohp-bi-test";
        //String cassandraHost = "ec2-54-68-90-205.us-west-2.compute.amazonaws.com";
        //String cassandraHost = "54.213.20.78";

        if (args != null && args.length == 1 && !args[0].isEmpty()) {
            cassandraHost = args[0];
        }
        
        System.out.println("Loading patient data to " + cassandraHost);

        PatientDataGenerator generator = new PatientDataGenerator();
        generator.connect(cassandraHost);
        // generator.createSchema();
        long start = System.currentTimeMillis();
        generator.loadData();
        System.out.println("Elapsed time for load: "
                + String.valueOf(System.currentTimeMillis() - start) + " ms");
        generator.close();
    }
}
