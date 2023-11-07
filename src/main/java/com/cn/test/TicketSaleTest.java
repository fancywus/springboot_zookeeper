package com.cn.test;

public class TicketSaleTest {

    public static void main(String[] args) {
        TicketSale sale = new TicketSale();

        Thread cTrip = new Thread(sale, "携程");
        Thread flyPig = new Thread(sale, "飞猪");

        cTrip.start();
        flyPig.start();
    }
}
