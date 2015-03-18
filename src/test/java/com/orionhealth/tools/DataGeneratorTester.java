package com.orionhealth.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.orionhealth.sample.DataGenerator;
import com.orionhealth.sample.ZipRange;

public class DataGeneratorTester {
	private DataGenerator dg = new DataGenerator(10, 2);
	
	@Test
	public void testBooleans() {
	    long yes = 0;
	    long no = 0;
	    for (int i = 0; i < 100000; i++) {
	        boolean value = dg.generateRandomBoolean();
	        if (value) yes++; 
	        else no++;
	    }
	    Assert.assertTrue(yes != 0);
	    Assert.assertTrue(no != 0);
	}

	@Test
	public void testHoppers() {
		// We should be able to draw the integers 1 through 10 from any
		// of the hoppers and receive an IllegalArgumentException on the
		// 11th
		List<Integer> s1 = new ArrayList<Integer>();
		for (int i = 1; i <= 10; i++) {
			s1.add(Integer.valueOf(i));
		}

		for (int i = 0; i < 10; i++) {
			s1.remove(Integer.valueOf(dg.generateRandomRanking(0)));
		}
		
		// There should be no values left in our test array
		if (s1.size() != 0){
			Assert.fail("Hopper 1 should have enumerated all values");
		}
		
		// Drawing from empty hopper should generate IllegalStateException
		try {
			dg.generateRandomRanking(0);
			Assert.fail("Should raise IllegalStateException");
		} catch (IllegalStateException ise) {
		} catch (Exception ex) {
			Assert.fail("Should raise IllegalStateException");
		}

		// Same tests for hopper 2
		List<Integer> s2 = new ArrayList<Integer>();
		for (int i = 1; i <= 10; i++) {
			s2.add(Integer.valueOf(i));
		}

		for (int i = 0; i < 10; i++) {
			s2.remove(Integer.valueOf(dg.generateRandomRanking(1)));
		}
		
		// There should be no values left in our test array
		if (s2.size() != 0){
			Assert.fail("Hopper 2 should have enumerated all values");
		}

		
		// Drawing from empty hopper should generate IllegalStateException
		try {
			dg.generateRandomRanking(1);
			Assert.fail("Should raise IllegalStateException");
		} catch (IllegalStateException ise) {
		} catch (Exception ex) {
			Assert.fail("Should raise IllegalStateException");
		}

		
		// Drawing from a non-existing hopper should generate IllegalStateException
		try {
			dg.generateRandomRanking(9);
			Assert.fail("Should raise IllegalStateException");
		} catch (IllegalStateException ise) {
		} catch (Exception ex) {
			Assert.fail("Should raise IllegalStateException");
		}
	}

	@Test
	public void testTIN() {
		// We randomly generate 100k SSNs and validate that they are
		// at least the right format
		String expression = "^\\d{3}[- ]?\\d{2}[- ]?\\d{4}$";
		Pattern pattern = Pattern.compile(expression);
		String tin = null;
		for (int i = 0; i < 1000000; i++) {
			tin = dg.generateRandomTIN();
			Matcher matcher = pattern.matcher(tin);
			if (!matcher.matches()) {
				Assert.fail("TIN found that was not valid: " + tin);
			}
		}
	}

	@Test
	public void testSSN() {
		// We randomly generate 100k SSNs and validate that they are
		// at least the right format
		String expression = "^\\d{3}[- ]?\\d{2}[- ]?\\d{4}$";
		Pattern pattern = Pattern.compile(expression);
		String ssn = null;
		for (int i = 0; i < 1000000; i++) {
			ssn = dg.generateRandomSSN();
			Matcher matcher = pattern.matcher(ssn);
			if (!matcher.matches()) {
				Assert.fail("SSN found that was not valid: " + ssn);
			}
		}
	}
	
	@Test
	public void testZipCode() {
	    // Here we test just the method for generating a zip code ten thousand
	    // times which tests that its able to generate that many random ones
	    // and still come up with a valid one, so we have no endless loops, or
	    // at least have some indication we will have none.
	    for (int i = 0; i < 10000; i++) {
	        String zip = dg.generateRandomZipCode();
	        Assert.assertNotNull("Zip cannot be null", zip);
	        Assert.assertFalse("Zip cannot be empty", zip.isEmpty());
	        Assert.assertTrue("Zip must have length 5", zip.length() == 5);
	    }
	    
	    // Here we test some of the zip ranges, so we use three ranges to verify
	    // that we are correctly generating and padding and that the zip that
	    // gets generated is actuall in the range we specified
	    List<ZipRange> ranges = new ArrayList<ZipRange>();
	    ranges.add(ZipRange.PR); // two leading zeros
	    ranges.add(ZipRange.CT); // one leading zero
	    ranges.add(ZipRange.CO); // no leading zero
	    
	    for (ZipRange range : ranges) {
	        String zip = dg.generateRandomZipCode(range.getLowerBound(), range.getUpperBound());
            Assert.assertNotNull("Zip cannot be null", zip);
            Assert.assertFalse("Zip cannot be empty", zip.isEmpty());
	        Assert.assertTrue("Zip outside specified range", Integer.parseInt(zip) >= range.getLowerBound());
	        Assert.assertTrue("Zip outside specified range", Integer.parseInt(zip) <= range.getUpperBound());
	        Assert.assertTrue("Zip must have length 5", zip.length() == 5);
	    }
	}
	
	@Test
	public void testName() {
        for (int i = 0; i < 10000; i++) {
            String name = dg.generateRandomName(i % 2 == 0);
            Assert.assertNotNull("Name cannot be null", name);
            Assert.assertFalse("Name cannot be empty", name.isEmpty());
            String nameParts[] = name.split(",");
            Assert.assertTrue("Name must be last, first", nameParts.length == 2);
        }	    
	}
}
