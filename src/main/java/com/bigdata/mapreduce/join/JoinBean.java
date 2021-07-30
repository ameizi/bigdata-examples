package com.bigdata.mapreduce.join;

import cn.hutool.json.JSONUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinBean implements Writable {

    private String customerId;
    private String name;
    private String address;
    private String phone;
    private String orderId;
    private String price;
    private String dataType;

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void set(String customerId, String name, String address, String phone, String orderId, String price, String dataType) {
        this.customerId = customerId;
        this.name = name;
        this.address = address;
        this.phone = phone;
        this.orderId = orderId;
        this.price = price;
        this.dataType = dataType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(customerId);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(address);
        dataOutput.writeUTF(phone);
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(price);
        dataOutput.writeUTF(dataType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.customerId = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.address = dataInput.readUTF();
        this.phone = dataInput.readUTF();
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readUTF();
        this.dataType = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }

}
