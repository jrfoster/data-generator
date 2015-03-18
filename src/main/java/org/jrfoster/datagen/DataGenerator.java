package org.jrfoster.datagen;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Seconds;

/**
 * This class will generate some randomized data for various things. Generally
 * speaking, this class is thread-safe.
 * <ul>
 * <li>It uses a census database to generate random last name/first name
 * combinations</li>
 * <li>It uses a PRNG to generate random lab test results</li>
 * <li>It uses a PRNG to generate other things, like ages, zip codes, et al</li>
 * <li>It uses Joda to create a random timestamp between two dates</li>
 * <li>It uses a static list of healthcare payers to generate a random payer</li>
 * </ul>
 * 
 * @author Jason Foster
 * 
 */
public class DataGenerator {
    private static final String FEMALE_FIRST_NAME_FILE = "./femaleFirstNames.txt";
    private static final String MALE_FIRST_NAME_FILE = "./maleFirstNames.txt";
    private static final String SURNAME_FILE = "./CSV_Database_of_Last_Names.csv";
    private static final String ZIP_FILE = "./primary_zipcodes.csv";

    private final Random rng = new Random(System.currentTimeMillis());
    private final AtomicInteger labSequencer = new AtomicInteger(50000);
    private final AtomicInteger procedureSequencer = new AtomicInteger(50000);
    private final AtomicInteger diagnosisSequencer = new AtomicInteger(50000);
    private final AtomicInteger encounterSequencer = new AtomicInteger(50000);
    private final List<Integer> patientIds = new ArrayList<Integer>();
    private final List<String> patientHicns = new ArrayList<String>();
    private final List<String> maleNames = new ArrayList<String>();
    private final List<String> femaleNames = new ArrayList<String>();
    private final List<String> surnames = new ArrayList<String>();
    private final Map<String, ZipData> zipCodes = new HashMap<String, ZipData>();
    private final int populationSize;
    private final Map<Integer, List<Integer>> hoppers = new HashMap<Integer, List<Integer>>();
    
    private final List<String> payers = Arrays.asList("AETNA", "AFLAC",
            "American Family Insurance", "American Medical Security",
            "American National Insurance Company", "Anthem Insurance",
            "Assurant, Inc.", "Asuris Northwest Health",
            "BlueCross BlueShield Association", "Celtic Insurance Company",
            "CIGNA", "College Health IPA", "Connecticare Inc.",
            "Continental General Insurance Company",
            "Golden Rule Insurance Company", "Group Health Cooperative",
            "Group Health Inc.", "Harvard Pilgrim Health Care",
            "Health Markets", "HUMANA", "Insurance Services of America",
            "Intermountain Healthcare", "Kaiser Permanente",
            "LifeWise Health Plan of Arizona",
            "LifeWise Health Plan of Oregon",
            "LifeWise Health Plan of Washington", "Medica Minnesota",
            "Medical Mutual", "Oregon Health Insurance",
            "Oxford Health Plans, Inc.", "Principal Financial Group, Inc.",
            "Shelter Insurance", "Unicare Health Insurance",
            "UnitedHealth Group Inc.", "Vista Health Plan",
            "Walter Jarvis Insurance Services", "WellPoint",
            "WPS Health Insurance");

    private final List<String> admitTypes = Arrays.asList("Accident",
            "Emergency", "Labor and Delivery", "Routine", "Newborn", "Urgent",
            "Elective");

    private final List<Integer> claimTypes = Arrays.asList(10, 20, 30, 40, 50,
            60, 61);

    private final List<Integer> facilityTypes = Arrays.asList(1, 2, 3, 4, 5, 6,
            7, 8, 9);
    
    // These are the ICD-9 DX codes for normal confirmed pregnancy
    private final List<String> pregnancyDxCodes = Arrays.asList("V22","V22.0","V22.1","V22.2");
    
    // These are the ICD-9 DX codes for IVD as defined by the IVD measures.  These exclude
    // the DX codes for AMI (410.x)
    private final List<String> IVDDxCodes = Arrays.asList("411","411.1","411.81","411.89","413",
            "413.1","413.9","414","414.01","414.02","414.03","414.04","414.05","414.06",
            "414.07","414.2","414.8","414.9","429.2","433","433.01","433.1","433.11",
            "433.2","433.21","433.3","433.31","433.8","433.81","433.9","433.91","434",
            "434.01","434.1","434.11","434.9","434.91","440.1","440.2","440.21","440.22",
            "440.23","440.24","440.29","444","440.4","444.1","444.21","444.22","444.81",
            "444.89","444.9","445.01","445.02","445.81","445.89");
    
    // These are the ICD-9 DX codes for AMI as defined by the IVD measures.
    private final List<String> IVDMIDxCodes = Arrays.asList("410.01","410.11","410.21",
            "410.31","410.41","410.51","410.61","410.71","410.81","410.91");
    
    // These are the ICD-9 DX codes for diabetes as defined by the DM measures. These
    // also include the fuller list of IVD DX codes that the DM measures use
    private final List<String> DMDxCodes = Arrays.asList("250.11","250.31","250.9",
            "250.23","250.72","250.43","250.81","250.73","250.63","648.02","250.91",
            "250.53","250.92","250.12","250.03","250.62","250","250.3","250.33",
            "250.71","250.4","250.41","250.21","250.61","250.2","250.8","250.83",
            "648.04","250.13","250.51","648.01","250.32","250.5","250.6","250.01",
            "250.93","250.7","250.02","648.03","250.52","250.42","250.82","250.1","250.22");
    
    // These are the ICD-9 DX codes for gestational diabetes as defined by the DM measures
    private final List<String> gestationalDMDxCodes = Arrays.asList("648.00","648.01",
            "648.02","648.03","648.80","648.80","648.81","648.82","648.83","648.84");
    
    // These are the Snomed proc codes for the IVD-related procedures 
    private final List<String> ivdProcCodes = Arrays.asList("3546002","10326007",
            "11101003","15256002","30670000","39202005","39724006","48431000",
            "74371005","75761004","80762004","82247006","85053006","91338001",
            "119564002","119565001","175007008","175008003","175009006","175011002",
            "175021005","175022003","175024002","175025001","175026000","175029007",
            "175030002","175031003","175032005","175033000","175045009","175047001",
            "175048006","175050003","175066001","232717009","232719007","232720001",
            "232721002","232722009","232723004","232724005","232727003","232728008",
            "232729000","265481001","275215001","275216000","275252001","275253006",
            "309814006","359597003","359601003","397193006","397431004","414088005",
            "414089002","414509005","415070008","418551006","419132001","428488008",
            "429499003","429639007","431759005");
    
    // These are HL7 discharge disposition codes as defined in CDA release 2
    // excluding those indicating patient death (20-29, 40, 41 and 42) since
    // the driver program only generates patients who are alive
    private final List<String> dischargeDispositions = Arrays.asList("01","02","03",
            "04","05","06","07","08","09","10","30");

    /**
     * Creates a new DataGenerator instance for a given population size and with
     * the given number of hoppers.<br>
     * <br>
     * We use the concept of randomly permuted integers pulled from a "hopper",
     * similar to how a lottery or bingo draws numbers, to generate the ranking.
     * 
     * @param populationSize
     *            the size of the initial patient population. This value is used
     *            for various other random data generation, like the range of
     *            values in the hoppers that can be used for ranking the
     *            population
     * @param numHoppers
     *            the number of hoppers of populationSize integers that will be
     *            created and randomly permuted to generate random rankings for
     *            the population
     */
    public DataGenerator(int populationSize, int numHoppers) {
        this.populationSize = populationSize;

        try {
            DataGenerator.loadNamesFromFile(surnames, SURNAME_FILE);
            DataGenerator.loadNamesFromFile(femaleNames, FEMALE_FIRST_NAME_FILE);
            DataGenerator.loadNamesFromFile(maleNames, MALE_FIRST_NAME_FILE);
            DataGenerator.loadZipsFromFile(zipCodes, ZIP_FILE);
        } catch (FileNotFoundException fnfex) {
            fnfex.printStackTrace();
        }

        // Based on the number of hoppers requested, we generate and shuffle an
        // integer
        // array and store them in an index-based map for use by the caller
        for (int i = 0; i < numHoppers; i++) {
            List<Integer> values = new ArrayList<Integer>(this.populationSize);
            for (int j = 0; j < this.populationSize; j++) {
                values.add(Integer.valueOf(j + 1));
            }

            Collections.shuffle(values);

            hoppers.put(Integer.valueOf(i), values);
        }
    }

    private static void loadNamesFromFile(List<String> target,
            String fileLocation) throws FileNotFoundException {
        Scanner s = null;

        try {
            s = new Scanner(new File(fileLocation));
            while (s.hasNextLine()) {
                target.add(s.nextLine());
            }
            System.out.println("Successfully loaded " + target.size()
                    + " names from " + fileLocation);
        } finally {
            if (s != null) {
                s.close();
            }
        }
    }

    private static void loadZipsFromFile(Map<String, ZipData> target,
            String fileLocation) throws FileNotFoundException {
        Scanner s = null;

        try {
            s = new Scanner(new File(fileLocation));
            while (s.hasNextLine()) {
                ZipData item = new ZipData(s.nextLine());
                target.put(item.getZipCode(), item);
            }
            System.out.println("Successfully loaded " + target.size()
                    + " zip codes from " + fileLocation);
        } finally {
            if (s != null) {
                s.close();
            }
        }
    }
    
    /**
     * Returns an interval representing the calendar year starting zero hour on
     * January 1 and ending at 11:59:59.999 of the provided century.
     * 
     * @param year
     *            The year for which to generate the measurement period
     * @return 
     *            Interval starting zero hour on January 1 and ending at 
     *            11:59:59.999 of the provided century
     */
    public Interval generateMeasurementPeriod(int year) {
        DateTime begin = new DateTime(year, 1, 1, 0, 0, 0, 0);
        DateTime end = new DateTime(year, 12, 31, 23, 59, 59, 999);
        return new Interval(begin, end);
    }

    /**
     * Convenience method to add a number of days to a start date
     * 
     * @param startDate
     *            Date to add days to
     * @param days
     *            number of days to add
     * 
     * @return new date the specified number of days after start date
     */
    public Date generateEndOfDuration(Date startDate, int days) {
        if (startDate == null)
            throw new IllegalArgumentException("startDate required");

        DateTime retVal = new DateTime(startDate.getTime()).plusDays(days);

        // We also randomize the time by either adding or subtracting a number
        // of seconds from the determined date.
        return generateRandomBoolean() ? retVal
                .minusSeconds(rng.nextInt(50000)).toDate() : retVal
                .plusSeconds(rng.nextInt(50000)).toDate();
    }

    /**
     * Convenience method to calculate the number of days between two dates
     * 
     * @param startDate
     *            lower bound of date range
     * @param endDate
     *            upper bound of date range
     * 
     * @return integer number of days in the given range, inclusive
     */
    public int generateDaysBetween(Date startDate, Date endDate) {
        if (endDate == null || startDate == null || (endDate.before(startDate)))
            throw new IllegalArgumentException(
                    "endDate must be after start date");

        return Days.daysBetween(new DateTime(startDate.getTime()),
                new DateTime(endDate.getTime())).getDays();
    }

    private int generateNextSequence(AtomicInteger seq) {
        return seq.incrementAndGet();
    }
    
    /**
     * Returns the next number in a given sequence. The sequence starts at 50000
     * and increments from there.
     * 
     * @return an int > 50000 guaranteed to be generated in sequence
     */
    public int generateNextEncounterSequence() {
        return generateNextSequence(encounterSequencer);
    }
    
    /**
     * Returns the next number in a given sequence. The sequence starts at 50000
     * and increments from there.
     * 
     * @return an int > 50000 guaranteed to be generated in sequence
     */
    public int generateNextProcedureSequence() {
        return generateNextSequence(procedureSequencer);
    }
    
    /**
     * Returns the next number in a given sequence. The sequence starts at 50000
     * and increments from there.
     * 
     * @return an int > 50000 guaranteed to be generated in sequence
     */
    public int generateNextLabSequence() {
        return generateNextSequence(labSequencer);
    }
    
    /**
     * Returns the next number in a given sequence. The sequence starts at 50000
     * and increments from there.
     * 
     * @return an int > 50000 guaranteed to be generated in sequence
     */
    public int generateNextDiagnosisSequence() {
        return generateNextSequence(diagnosisSequencer);
    }
    
    /**
     * This method is used to generate a random UUID to use for hicn to mrn
     * xref.
     * 
     * @return UUID
     */
    public String generateRandomUUID() {
        return UUID.randomUUID().toString();
    }
    

    /**
     * Generates a random integer that can be used to identify a patient. A side
     * effect of calling this method is that the identifier is stored internally
     * in a list so that it can be guaranteed to always return an unused value.
     * 
     * @return the newly generated identifier. It is up to the caller to manage
     *         the id returned by this method, as there is no way to retrieve it
     *         once its been generated.
     */
    public int generateRandomIdentifier() {
        Integer proposedId = null;
        synchronized (patientIds) {
            do {
                proposedId = Integer.valueOf(rng
                        .nextInt(((1100000 - 100000) + 1) + 100000));
            } while (patientIds.contains(proposedId));

            patientIds.add(proposedId);
        }

        return proposedId;
    }

    /**
     * Generates a random number less than the max provided by the caller
     * 
     * @param max
     * @return
     */
    public int generateRandomCount(int max) {
        int proposed = rng.nextInt(max);
        return proposed == 0 ? 1 : proposed;
    }

    /**
     * Returns a string containing a random lastname/firstname combination using
     * the census database.
     * 
     * @return a string formatted in "lastname, firstname" format
     */
    public String generateRandomName(boolean male) {
        StringBuilder sb = new StringBuilder();
        sb.append(surnames.get(rng.nextInt(surnames.size())))
                .append(", ")
                .append(male ? maleNames.get(rng.nextInt(maleNames.size()))
                        : femaleNames.get(rng.nextInt(femaleNames.size())));
        return sb.toString();
    }

    /**
     * Returns a string containing a random surname from the census list of
     * surnames in the U.S.
     * 
     * @return String with a random surname
     */
    public String generateRandomSurname() {
        return surnames.get(rng.nextInt(surnames.size()));
    }

    /**
     * Returns a string containing either a gender-specific given name or the
     * first letter thereof from the list of given names from the U.S. census
     * 
     * @param male
     *            flag indicating whether or not to generate a male name
     * @param initial
     *            flag indicating whether to return a middle initial or name
     * @return a String containing either an initial or a name matching gender
     */
    public String generateRandomGivenName(boolean male, boolean initial) {
        String name = generateRandomGivenName(male);
        return initial ? name.substring(1, 1) : name;
    }

    /**
     * Returns a string containing a gender-specific given name from the list of
     * given names from the U.S. census.
     * 
     * @param male
     *            whether to return a male or female surname
     * @return String containing a gender-specific given name
     */
    public String generateRandomGivenName(boolean male) {
        return male ? maleNames.get(rng.nextInt(maleNames.size()))
                : femaleNames.get(rng.nextInt(femaleNames.size()));
    }

    /**
     * Returns a random age between 18 and 85
     * 
     * @return integer age between 18 and 85
     */
    public int generateRandomAge() {
        return generateRandomAge(18, 85);
    }

    /**
     * Returns a random age between two specified ages
     * 
     * @param min
     *            lower bound of age range
     * @param max
     *            upper bound of age range
     * @return integer age within the range given
     */
    public int generateRandomAge(int min, int max) {
        if (min > max) {
            throw new IllegalArgumentException("max must exceed min");
        }

        return rng.nextInt((max - min) + 1) + min;
    }

    /**
     * Uses a simple modulus on a random number to provide a random gender code
     * 
     * @return the string "M" or "F"
     */
    public String generateRandomGender() {
        if (generateRandomBoolean()) {
            return "F";
        } else {
            return "M";
        }
    }

    /**
     * Uses a simple modulus on a random number to provide a random boolean
     * 
     * @return random boolean value
     */
    public boolean generateRandomBoolean() {
        return rng.nextLong() % 2 == 0;
    }
    
    /**
     * Generates a random ZipData object using data from a file containing zip 
     * codes from the USPS.
     * 
     * @return a ZipData instance corresponding to a valid, non-decommissioned zip code
     */
    public ZipData generateRandomZipData() {
        return generateRandomZipData(0, 99999);
    }
    
    /**
     * Generates a random ZipData object within a range.  Can be useful when trying to cluster
     * data around a given city or region.
     * 
     * @param min lower bound of zip code range
     * @param max upper bound of zip code range
     * @return random ZipData within the range specified
     */
    public ZipData generateRandomZipData(int min, int max) {
        ZipData zipData = null;

        do {
            zipData = zipCodes.get(StringUtils.leftPad(String.valueOf(rng.nextInt((max - min) + 1) + min), 5, '0'));
        } while (zipData == null || zipData.isDecommisioned());

        return zipData;
    }
    
    /**
     * Generates a random zip code from a file containing zip codes from the USPS
     * 
     * @return a valid, non-decommissioned zip code
     */
    public String generateRandomZipCode() {
        ZipData data = generateRandomZipData();
        return data.getZipCode();
    }
    
    /**
     * Generates a random zip code within a range from a file containing zip codes from
     * the USPS
     * 
     * @param min lower bound of the zip code range
     * @param max upper bound of the zip code range
     * @return random zip within the given range
     */
    public String generateRandomZipCode(int min, int max) {
        ZipData data = generateRandomZipData(min, max);
        return data.getZipCode();
    }

    /**
     * Returns a random value from the list of payers
     * 
     * @return string payer name chosen at random
     */
    public String generateRandomPayer() {
        return payers.get(rng.nextInt(payers.size()));
    }

    /**
     * Generates a properly-formatted but invalid SSN for testing purposes. The
     * string returned has a region code (first three characters) in the range
     * 900-999 which are not valid SSNs.
     * 
     * @return String containing an invalid but properly formatted SSN
     */
    public String generateRandomSSN() {
        return new StringBuffer()
                .append(StringUtils.leftPad(
                        String.valueOf(rng.nextInt(100) + 900), 3, '0'))
                .append("-")
                .append(StringUtils.leftPad(String.valueOf(rng.nextInt(100)),
                        2, '0'))
                .append("-")
                .append(StringUtils.leftPad(String.valueOf(rng.nextInt(10000)),
                        4, '0')).toString();
    }

    /**
     * Generates a properly-formatted but invalid HICN for testing purposes. An
     * HICN contains two parts, the first part is the claim account number,
     * which reflects the policy number of the person who has earned the
     * Medicare benefits. The other part, the beneficiary identification code,
     * identifies the current relationship of the beneficiary to the wage
     * earner. This number is typically two digits--one letter and one number.<br>
     * <br>
     * This code will not generate a valid second part, since for testign
     * purposes we don't really need a valid HICN, we just need one that is
     * formatted properly. <br>
     * Changes to a person’s health insurance claim number occur when the
     * enrollee’s relationship to the wage earner changes. For example, the
     * claim number would change after a shift from “spouse” to “widow.”
     * 
     * @return String containing the HICN
     */
    public String generateRandomHICN() {
        String proposedId = null;
        synchronized (patientHicns) {
            do {
                proposedId = new StringBuffer()
                        .append(generateRandomSSN())
                        .append("-")
                        .append(RandomStringUtils.randomAlphabetic(1)
                                .toUpperCase())
                        .append(RandomStringUtils.randomAlphanumeric(1)
                                .toUpperCase()).toString();
            } while (patientIds.contains(proposedId));

            patientHicns.add(proposedId);
        }

        return proposedId;
    }

    /**
     * Returns a random, correctly-formatted but invalid TIN suitable for
     * testing purposes.<br>
     * <br>
     * Like an SSN, a TIN is a nine-digit number. But, a TIN always begins with
     * 9 and has a number generally are in the range 70-99 (excluding 89 and 93)
     * for its 4th and 5th digits. So, this routine will use those two
     * exclusions for its 4th and 5th digits to produce an invalid TIN.
     * 
     * @return ten-character string of random alpha-numeric characters
     */
    public String generateRandomTIN() {
        return new StringBuffer()
                .append(StringUtils.leftPad(
                        String.valueOf(rng.nextInt(100) + 900), 3, '0'))
                .append("-")
                .append(generateRandomBoolean() ? "89" : "93")
                .append("-")
                .append(StringUtils.leftPad(String.valueOf(rng.nextInt(10000)),
                        4, '0')).toString();
    }
    
    /**
     * Returns a random discharge disposition, including those indicating the
     * patient expired (died).
     * 
     * @return String disposition discharge code
     */
    public String generateRandomDischargeDisposition() {
        return dischargeDispositions.get(rng.nextInt(dischargeDispositions.size()));
    }
    
    public CodedValue generateRandomPregnancyDiagnosis() {
        // We want to limit the incidence of pregnancy to about 25% of requests
        if (rng.nextFloat() <= 0.25f) {
            CodedValue dxCode = new CodedValue();
            dxCode.setCodingSystem("I9");
            dxCode.setIdentifier(pregnancyDxCodes.get(rng.nextInt(pregnancyDxCodes.size())));
            return dxCode;
        } else {
            return null;
        }
    }
    
    public CodedValue generateRandomIVDDiagnosis(boolean isAMI) {
        if (isAMI) {
            CodedValue dxCode = new CodedValue();
            dxCode.setCodingSystem("I9");
            dxCode.setIdentifier(IVDMIDxCodes.get(rng.nextInt(IVDMIDxCodes.size())));
            return dxCode;
        } else {
            return generateRandomIVDDiagnosis();
        }
    }
    
    public CodedValue generateRandomIVDDiagnosis() {
        CodedValue dxCode = new CodedValue();
        dxCode.setCodingSystem("I9");
        dxCode.setIdentifier(IVDDxCodes.get(rng.nextInt(IVDDxCodes.size())));
        return dxCode;
    }
    
    public CodedValue generateRandomDMDiagnosis(boolean isMale) {
        // Since we cannot make every female have gestational diabetes, we
        // further randomize on who will get it based on an approximate
        // incidence rate of less than 10%
        if (!isMale && rng.nextFloat() <= 0.10f) {
            CodedValue dxCode = new CodedValue();
            dxCode.setCodingSystem("I9");
            dxCode.setIdentifier(gestationalDMDxCodes.get(rng.nextInt(gestationalDMDxCodes.size())));
            return dxCode;
        } else {
            return generateRandomDMDiagnosis();
        }
    }
    
    public CodedValue generateRandomDMDiagnosis() {
        CodedValue dxCode = new CodedValue();
        dxCode.setCodingSystem("I9");
        dxCode.setIdentifier(DMDxCodes.get(rng.nextInt(DMDxCodes.size())));
        return dxCode;
    }
    
    /**
     * Returns a random Snomed procedure code corresponding to one of the IVD
     * procedure codes for coronoary artery bypass graft (CABG) or percutaneous 
     * transluminal coronary angioplasty (PTCA) as a CodedValue
     * 
     * @return random Code code for a procedure
     */
    public CodedValue generateRandomIVDProcCode() {
        CodedValue code = new CodedValue();
        code.setIdentifier(ivdProcCodes.get(rng.nextInt(ivdProcCodes.size())));
        code.setCodingSystem("SNM");
        return code;
    }

    /**
     * Returns a random double suitable for use as a hemoglobin result.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result (<
     *            12.0)
     * 
     * @return a double representing a hemoglobin result
     */
    public double generateRandomHemoglobinResult(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value between 0 and 12,
            // exclusive
            return (double) rng.nextInt(120) / 10d;
        } else {
            // Normal range for men is 13.5 to 17.5 and the normal range
            // for women is 12.0 to 15.5, but for our purposes we don't
            // care about gender when generating a random result so our
            // range is 12.0 to 17.5.
            return (double) (rng.nextInt((175 - 120) + 1) + 120) / 10d;
        }
    }
    
    /**
     * Generates a random lipid panel
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal results panel
     *            
     * @return LipidPanelResult with random test values 
     */
    public LipidPanelResult generateRandomLipidPanelResult(Date date, boolean isAbnormal) {
        LipidPanelResult result = new LipidPanelResult(date, generateRandomHDLCResult(isAbnormal),
                generateRandomLDLCResult(isAbnormal),
                generateRandomTRIGResult(isAbnormal));
        
        return result;
    }

    /**
     * Generates a random double suitable for use as an LDLC result.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result (100 <
     *            n < 200)
     * 
     * @return a double representing a ldlc result
     */
    public double generateRandomLDLCResult(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value between 100 and
            // 200
            return (double) (rng.nextInt(200 - 99) + 99);
        } else {
            // Normal range is between 0 and 100 exclusive
            return (double) rng.nextInt(100);
        }
    }
    
    /**
     * Generates a random double suitable for use as an HDLC result.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result (n < 40)
     * @return
     */
    public double generateRandomHDLCResult(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value less than 40
            return (double) (rng.nextInt(40));
        } else {
            // Normal range is between 40 and 75
            return (double) (rng.nextInt(75 - 40) + 40);
        }
    }
    
    /**
     * Generates a random double suitable for use as a Triglycerides result.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result (n > 199)
     * 
     * @return
     */
    public double generateRandomTRIGResult(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value between 200 and
            // 500
            return (double) (rng.nextInt(500 - 199) + 199);
        } else {
            // Normal range is a value between 0 and 200, exclusive
            return (double) (rng.nextInt(200));
        }
    }

    /**
     * Returns a random integer suitable for use as a diastolic blood pressure.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result. For
     *            diastolic blood pressure, an abnormal result is either a value
     *            less than 60 or a value between 90 and 200
     * @return
     */
    public double generateRandomDiastolicBP(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value either less than
            // 60 or a value between 90 and less than 200.
            if (generateRandomBoolean()) {
                return (double) (rng.nextInt(60));
            } else {
                return (double) (rng.nextInt(200 - 90) + 90);
            }
        } else {
            // Normal range is between 60 and 90, exclusive
            return (double) (rng.nextInt(90 - 60) + 60);
        }
    }

    /**
     * Returns a random integer suitable for use as a systolic blood pressure.
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result. For
     *            systolic blood pressure, an abnormal result is either
     *            hypotension which is a value less than 90 or hypertension
     *            which is a value greater than 139 and less than 180
     * @return
     */
    public double generateRandomSystolicBP(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value either less than
            // 90 or a value between 140 and less than 250.
            if (generateRandomBoolean()) {
                return (double) (rng.nextInt(90));
            } else {
                return (double) (rng.nextInt(250 - 140) + 140);
            }
        } else {
            // Normal range is between 90 and 139
            return (double) (rng.nextInt(139 - 90) + 90);
        }
    }

    /**
     * Returns a random integer suitable for use as a sodium result
     * 
     * @param isAbnormal
     *            whether the caller wishes to receive an abnormal result (n <
     *            135)
     * 
     * @return a double representing a sodium result
     */
    public int generateRandomSodiumResult(boolean isAbnormal) {
        if (isAbnormal) {
            // For our purposes, an abnormal result is a value between 0 and
            // 135, exclusive
            return rng.nextInt(135);
        } else {
            // Normal range is 135 to 145
            return (rng.nextInt(145 - 135) + 135);
        }
    }

    /**
     * Returns a random Java date between the minDate and maxDate. Note that the
     * value cannot take into account time zone for the date when the caller
     * formats it, so its possible that when formatting a date returned from
     * this method it could fall outside the given range because of time zone
     * offset.
     * 
     * @param minDate
     *            lower bound of the date range
     * @param maxDate
     *            upper bound of the date range
     * @param includeTime
     *            flag to indicate whether to include an actual timestamp or
     *            00:00:00
     * @return date within the range given, with a caveat on timezone
     */
    public Date generateRandomTimestamp(Date minDate, Date maxDate) {
        if (minDate == null || maxDate == null || maxDate.before(minDate))
            throw new IllegalArgumentException("maxDate must be after minDate");

        DateTime minDT = new DateTime(minDate.getTime());
        DateTime maxDT = new DateTime(maxDate.getTime());
        DateTime randomDate = null;

        // We actually have to take a different approach to generating a date
        // within a wide range as we do when generating a date within a narrow
        // range. We use the largest value of seconds that Joda can manage to
        // make the decision as to which type of range we are dealing with, and
        // so the dividing line is at just over 68 years.
        if (maxDT.minusSeconds(Integer.MAX_VALUE).isBefore(minDT.getMillis())) {
            // Here we have a "narrow" range
            Minutes minimumPeriod = Minutes.minutes(15);
            int minPeriodSecs = minimumPeriod.toStandardSeconds().getSeconds();
            int maxPeriodSecs = Seconds.secondsBetween(minDT, maxDT)
                    .getSeconds();

            Seconds randomPeriod = Seconds.seconds(rng.nextInt(maxPeriodSecs
                    - minPeriodSecs));
            randomDate = minDT.plus(minimumPeriod).plus(randomPeriod);
        } else {
            // Here we have a "wide" range
            Days minimumPeriod = Days.days(30);
            int minPeriodHours = minimumPeriod.get(DurationFieldType.hours());
            int maxPeriodHours = Hours.hoursBetween(minDT, maxDT).getHours();

            Hours randomPeriod = Hours.hours(rng.nextInt(maxPeriodHours
                    - minPeriodHours));
            randomDate = minDT.plus(minimumPeriod).plus(randomPeriod);
        }

        // This little bit ensures that we don't always get the same HH:MM:SS
        // while guaranteeing that we don't return a date outside the given
        // range. Of course this can get complex when dealing with timezone
        // offsets, but this isn't for production :)
        int randomHours = rng.nextInt(720);
        DateTime retVal;
        if (randomHours % 2 == 0) {
            retVal = randomDate.plusHours(randomHours);
            return retVal.isAfter(maxDT) ? maxDate : retVal.toDate();
        }

        retVal = randomDate.minusHours(randomHours);
        return retVal.isBefore(minDT) ? minDate : retVal.toDate();
    }

    /**
     * Returns a random string suitable for use as an admit type on an
     * encounter.
     * 
     * @return random string admit type
     */
    public String generateRandomAdmitType() {
        return admitTypes.get(rng.nextInt(admitTypes.size()));
    }

    /**
     * Returns a random integer claim type code for a CCLF file
     * 
     * @return random integer claim type
     */
    public int generateRandomClaimType() {
        return claimTypes.get(rng.nextInt(claimTypes.size()));
    }

    /**
     * Returns a random integer facility type code for a CCLF file
     * 
     * @return random integer facility type
     */
    public int generateRandomFacilityType() {
        return facilityTypes.get(rng.nextInt(facilityTypes.size()));
    }

    /**
     * The creator of this class requested hoppers to be created when the
     * instance was constructed, and hoppers are generated up to the number of
     * patients in the population size requested during construction. This
     * method essentially removes a number from the top of the selected hopper
     * and returns the integer. When there are no more numbers in the hopper, an
     * IllegalStateException is raised.<br>
     * <br>
     * Calling this method removes a value from the hopper at a given index, so
     * the caller should keep track of the number of times this method has been
     * called for a given index if the caller wishes to avoid exceptions.
     * 
     * @param index
     *            index of the hopper from which to draw.
     * 
     * @return the integer drawn from the given hopper.
     * 
     * @throws IllegalStateException
     *             if no hopper exists at the given index or if all the values
     *             have been removed from a given hopper
     */
    public int generateRandomRanking(int index) throws IllegalStateException {
        try {
            Integer value = hoppers.get(Integer.valueOf(index)).remove(0);
            if (value == null) {
                throw new IllegalStateException(
                        "No value at index 0 found in hopper at index " + index);
            }
            return value;
        } catch (UnsupportedOperationException uoe) {
            throw new IllegalStateException(
                    "Programmer error - remove not supported");
        } catch (NullPointerException npe) {
            throw new IllegalStateException(
                    "No hopper defined for the given index");
        } catch (IndexOutOfBoundsException ex) {
            throw new IllegalStateException("Hopper at given index is empty");
        }
    }
    

}
