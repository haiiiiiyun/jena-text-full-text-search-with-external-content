package com.atjiang.jena;


public class App
{
    static String EMAIL_URI_PREFIX = "http://atjiang.com/data/email/";
    public static void main( String[] args ) {

        testJenaText();
    }

    public static void testJenaText() {
        IndexFiles.testIndex();
        try {
            SearchFiles.testSearch();
        } catch (Exception e){

            System.out.println(e.toString());
        }

        JenaTextSearch.main();
    }
}
