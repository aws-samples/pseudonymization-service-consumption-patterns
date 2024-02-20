package com.amazonaws.samples;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import com.google.gson.Gson;


public class PseudonymizerUDF extends UserDefinedFunctionHandler {
    private static final String SOURCE_TYPE = "MyCustomUDF";
    public PseudonymizerUDF() {
        super(SOURCE_TYPE);
    }

    private Gson gson = new Gson();

    private String getSecret(String secretName, String region) {

        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(Region.of(region))
                .build();

        String secret = null;

        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            secret = valueResponse.secretString();
        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        secretsClient.close();
        return secret;
    }

    private String getStringResponse(String input, String url) throws URISyntaxException, IOException, InterruptedException {
        final String secretName = System.getenv("SECRET_NAME");
        final String secretRegion = System.getenv("SECRET_REGION");

        String accessToken = getSecret(secretName, secretRegion);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(url))
                .header("x-api-key", accessToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(input))
                .build();

        HttpResponse<String> response = HttpClient.newBuilder()
                .build()
                .send(request, HttpResponse.BodyHandlers.ofString());

        return response.body();
    }

    public List<String> reidentify(List<String> input) throws URISyntaxException, IOException, InterruptedException {

        Pseudonyms pseudonyms = new Pseudonyms();
        pseudonyms.setPseudonyms(input);
        String jsonPseudonyms = gson.toJson(pseudonyms);

        String url = System.getenv("EP_URL") + "/Dev/reidentification";
        String response = getStringResponse(jsonPseudonyms, url);

        Identifiers identifiers = gson.fromJson(response, Identifiers.class);
        System.out.println(identifiers.getIdentifiers());
        return identifiers.getIdentifiers();
    }

    public List<String> pseudonymize(List<String> input, String deterministic) throws URISyntaxException, IOException, InterruptedException {

        Identifiers identifiers = new Identifiers();
        identifiers.setIdentifiers(input);
        String jsonIdentifiers = gson.toJson(identifiers);

        String url = System.getenv("EP_URL") + "/Dev/pseudonymization?deterministic=" + deterministic;
        String response = getStringResponse(jsonIdentifiers, url);

        Pseudonyms pseudonyms = gson.fromJson(response, Pseudonyms.class);
        return pseudonyms.getPseudonyms();
    }

}
