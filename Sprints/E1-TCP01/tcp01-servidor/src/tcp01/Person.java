package tcp01;

import java.io.Serializable;


public class Person implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private String name;
    private int year;

    public Person(String name, int year) {
        this.name = name;
        this.year = year;
    }

    public String getName() {
        return name;
    }

    public int getYear() {
        return year;
    }
}