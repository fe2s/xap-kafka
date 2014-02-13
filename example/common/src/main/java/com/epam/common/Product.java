package com.epam.common;

import com.gigaspaces.document.SpaceDocument;

/**
 * Created by Mykola_Zalyayev on 10.02.14.
 */

/**
 * A simple extended space document used to work with the Space. Demonstrate set topic name in which write kafka producer.
 */
public class Product extends SpaceDocument {
    private static final String TYPE_NAME = "Product";
    private static final String PROPERTY_CATALOG_NUMBER = "CatalogNumber";
    private static final String PROPERTY_NAME = "Name";
    private static final String PROPERTY_PRICE = "Price";
    private static final String DEFAULT_SPACE_DOCUMENT_KAFKA_TOPIC = "spaceDocument.kafka.topic";

    /*
     * Set property to SpaceDocument topic name. Default property name is "spaceDocument.kafka.topic"
     */
    public Product() {
        super(TYPE_NAME);
        super.setProperty(DEFAULT_SPACE_DOCUMENT_KAFKA_TOPIC, "product");
    }

    public String getCatalogNumber() {
        return super.getProperty(PROPERTY_CATALOG_NUMBER);
    }
    public Product setCatalogNumber(String catalogNumber) {
        super.setProperty(PROPERTY_CATALOG_NUMBER, catalogNumber);
        return this;
    }

    public String getName() {
        return super.getProperty(PROPERTY_NAME);
    }
    public Product setName(String name) {
        super.setProperty(PROPERTY_NAME, name);
        return this;
    }

    public float getPrice() {
        return super.getProperty(PROPERTY_PRICE);
    }
    public Product setPrice(float price) {
        super.setProperty(PROPERTY_PRICE, price);
        return this;
    }
}
