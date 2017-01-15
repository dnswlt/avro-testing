package de.reondo.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dw on 15.01.17.
 */
public class App {

    public static void main(String[] args) throws IOException {
        // (1) Invalid customer: .name missing
        Customer c1 = new Customer();
        c1.setFavoriteColor("blue");
        c1.setFavoriteNumber(17);

        // (2) Create via builder. NOTE: must set all fields without default, even when null!
        Customer c2 = Customer.newBuilder().setName("Peter").setFavoriteColor("red")
                .setFavoriteNumber(null).build();

        // (3) Create "classically", this time with all required fields set
        Customer c3 = new Customer();
        c3.setName("Jens");
        c3.setHasSubscription(false); // overriding default
        c3.setFavoriteColor("purple");

        // Write them into file "customers.avro", including their schema
        DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter);
        File avroFile = new File("customers.avro");
        dataFileWriter.create(c1.getSchema(), avroFile);

        for (Customer c : Arrays.asList(c1, c2, c3)) {
            try {
                dataFileWriter.append(c);
                System.out.printf("Appended %s successfully\n", c);
            } catch (Exception e) {
                System.out.printf("ERROR: Customer %s is invalid\n", c);
            }
        }
        dataFileWriter.close();

        // Read them back
        DatumReader<Customer> userDatumReader = new SpecificDatumReader<>(Customer.class);
        DataFileReader<Customer> dataFileReader = new DataFileReader<>(avroFile, userDatumReader);
        List<Customer> customers = new ArrayList<>();
        Customer customer = null;
        while (dataFileReader.hasNext()) {
            // Just to show it: Reuse Customer object by passing it to next().
            customer = dataFileReader.next(customer);
            System.out.printf("Read back %s\n", customer);
            // Useless in combination with reuse, but again: just to show the feature: "copy constructor"
            customers.add(Customer.newBuilder(customer).build());
        }
        System.out.printf("Read back %d customers from file: %s\n", customers.size(), customers.stream()
                .map(c -> c.getName()).collect(Collectors.toList()));
    }
}
