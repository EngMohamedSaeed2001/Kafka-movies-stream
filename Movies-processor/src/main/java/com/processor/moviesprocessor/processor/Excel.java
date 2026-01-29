package com.processor.moviesprocessor.processor;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Excel {

    public void write(){
        List<Object[]> moviesData = new ArrayList<>();
        moviesData.add(new Object[]{"Title", "Genre", "Release Date"});
        //moviesData.add(new Object[]{"John Doe", 101, "2023-01-15", 55000.0});



        try (Workbook workbook = new XSSFWorkbook()) {

        Sheet sheet = workbook.createSheet("Movies-genre");

        int rowNum = 0;
        for (Object[] dataArr : moviesData) {
            Row row = sheet.createRow(rowNum++);
            int colNum = 0;
            for (Object field : dataArr) {
                Cell cell = row.createCell(colNum++);
                if (field instanceof String) {
                    cell.setCellValue((String) field);
                }
            }
        }


        try (FileOutputStream out = new FileOutputStream("Movies-genre.xlsx")) {
            workbook.write(out);
        }

        System.out.println("Movies-genre.xlsx file written successfully!");

    } catch (IOException e) {
        e.printStackTrace();
    }
}
}
