package com.amazonaws.samples;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for Athena UDF
 */
public class PseudonymizerUDFTest {
    private PseudonymizerUDF pseudonymizer;

    @Before
    public void setup() {
        this.pseudonymizer = new PseudonymizerUDF();
    }

    @Test
    public void testReidentification() throws IOException, URISyntaxException, InterruptedException {

        List<String> pseudonyms = new ArrayList<>(List.of("ODUxZTc1YTg1NzUyF93SzQSd3yQ0+XyFDEftHENuNr3+QaDsai6dCBCxxCHN",
                "MzVmYzNkZDQwMzlmPGA+Y4dybvi5Q/+i6zxnLoFRhDpLlx241hfCAkgYbkCG"));

        List<String> testOutput = new ArrayList<>(List.of("HFW5636AZWOUJ2PAM","4H46H0SXAI3MM49S5"));

        List<String> result = pseudonymizer.reidentify(pseudonyms);
        System.out.println("----------------------------------------------------------");
        System.out.println(result);
        System.out.println("----------------------------------------------------------");

        assertEquals(result, testOutput);
    }

    @Test
    public void testPseudonymization() throws IOException, URISyntaxException, InterruptedException {

        List<String> identifiers = new ArrayList<>(List.of("HFW5636AZWOUJ2PAM", "4H46H0SXAI3MM49S5"));
        List<String> testOutput = new ArrayList<>(List.of("ODUxZTc1YTg1NzUyF93SzQSd3yQ0+XyFDEftHENuNr3+QaDsai6dCBCxxCHN",
                "MzVmYzNkZDQwMzlmPGA+Y4dybvi5Q/+i6zxnLoFRhDpLlx241hfCAkgYbkCG"));

        List<String> result = pseudonymizer.pseudonymize(identifiers, "true");
        System.out.println("----------------------------------------------------------");
        System.out.println(result);
        System.out.println("----------------------------------------------------------");

        assertEquals(result, testOutput);
    }

}
