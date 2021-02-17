package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class IndexPerson implements DataSerializable {

    private long age;
    private long personId;
    private long count = 0;
    private double salary;
    private String firstName;
    private String lastName;

    public IndexPerson() {
        // No-op.
    }

    public IndexPerson(long personId, long age, String firstName, String lastName, double salary) {
        this.personId = personId;
        this.age = age;
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }

    public long getPersonId() {
        return personId;
    }

    public void setPersonId(long personId) {
        this.personId = personId;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public boolean equals(Object o) {
        return this == o || (o instanceof IndexPerson)
                && personId == ((IndexPerson) o).personId;
    }

    public int hashCode() {
        return (int) personId;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(personId);
        out.writeLong(age);
        out.writeString(firstName);
        out.writeString(lastName);
        out.writeDouble(salary);
        out.writeLong(count);
    }

    public void readData(ObjectDataInput in) throws IOException {
        personId = in.readLong();
        age = in.readLong();
        firstName = in.readString();
        lastName = in.readString();
        salary = in.readDouble();
        count = in.readLong();
    }

    @Override
    public String toString() {
        return "IndexPerson{"
                + "personId=" + personId
                + ", age=" + age
                + ", firstName='" + firstName + '\''
                + ", lastName='" + lastName + '\''
                + ", salary=" + salary
                + ", count=" + count
                + '}';
    }
}
