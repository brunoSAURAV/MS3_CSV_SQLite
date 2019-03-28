package com.SAURAV;
/*
Data model for CSV file
 */
public class ms3Data {

    // Setting up data model
    String A;
    String B;
    String C;
    String D;
    String E;
    String F;
    String G;
    String H;
    String I;
    String J;

    // This is where we form a constructor which is going to set default values we have gotten after getters and setters

    public ms3Data(String a, String b, String c, String d, String e, String f, String g, String h, String i, String j) {
        A = a;
        B = b;
        C = c;
        D = d;
        E = e;
        F = f;
        G = g;
        H = h;
        I = i;
        J = j;
    }
    // getter and setters for respective column headers

    public String getA() {
        return A;
    }

    public void setA(String a) {
        A = a;
    }

    public String getB() {
        return B;
    }

    public void setB(String b) {
        B = b;
    }

    public String getC() {
        return C;
    }

    public void setC(String c) {
        C = c;
    }

    public String getD() {
        return D;
    }

    public void setD(String d) {
        D = d;
    }

    public String getE() {
        return E;
    }

    public void setE(String e) {
        E = e;
    }

    public String getF() {
        return F;
    }

    public void setF(String f) {
        F = f;
    }

    public String getG() {
        return G;
    }

    public void setG(String g) {
        G = g;
    }

    public String getH() {
        return H;
    }

    public void setH(String h) {
        H = h;
    }

    public String getI() {
        return I;
    }

    public void setI(String i) {
        I = i;
    }

    public String getJ() {
        return J;
    }

    public void setJ(String j) {
        J = j;
    }

}
