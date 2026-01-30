package com.processor.moviesprocessor.processor;

import com.processor.moviesprocessor.model.Movie;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.poi.ss.usermodel.*;

public class Excel {
    public String writeMoviesToExcel(ArrayList<Movie> movies, String genre) {
        String filename = "Movies-" + genre + "-" + System.currentTimeMillis() + ".xlsx";

        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Movies-" + genre);

            // Create header style
            CellStyle headerStyle = workbook.createCellStyle();
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);

            // Create header row
            Row headerRow = sheet.createRow(0);
            String[] headers = {"Title", "Genres", "Release Date"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(headerStyle);
            }

            // Write movie data
            int rowNum = 1;
            for (Movie movie : movies) {
                Row row = sheet.createRow(rowNum++);

                row.createCell(0).setCellValue(movie.getOriginalTitle() != null ? movie.getOriginalTitle() : "");
                row.createCell(1).setCellValue(movie.getGenres() != null ? String.join(", ", movie.getGenres()) : "");
                row.createCell(2).setCellValue(movie.getReleaseDate() != null ? movie.getReleaseDate() : "");
            }

            // Auto-size columns
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            // Write to file
            try (FileOutputStream out = new FileOutputStream(filename)) {
                workbook.write(out);
            }

            System.out.println("Excel file written successfully: " + filename);
            return filename;

        } catch (IOException e) {
            System.err.println(" Error writing Excel file: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public String writeMultipleBatchesToExcel(List<ArrayList<Movie>> batches) {
        String filename = "Movies-" + "details-" + System.currentTimeMillis() + ".xlsx";

        try (Workbook workbook = new XSSFWorkbook()) {

            // Create a sheet for each batch
            int batchNum = 1;
            for (ArrayList<Movie> batch : batches) {
                Sheet sheet = workbook.createSheet("Batch-" + batchNum);

                // Create header
                CellStyle headerStyle = workbook.createCellStyle();
                Font headerFont = workbook.createFont();
                headerFont.setBold(true);
                headerStyle.setFont(headerFont);

                Row headerRow = sheet.createRow(0);
                String[] headers = {"Title", "Genres", "Release Date"};
                for (int i = 0; i < headers.length; i++) {
                    Cell cell = headerRow.createCell(i);
                    cell.setCellValue(headers[i]);
                    cell.setCellStyle(headerStyle);
                }

                // Write batch data
                int rowNum = 1;
                for (Movie movie : batch) {
                    Row row = sheet.createRow(rowNum++);
                    row.createCell(0).setCellValue(movie.getOriginalTitle() != null ? movie.getOriginalTitle() : "");
                    row.createCell(1).setCellValue(movie.getGenres() != null ? String.join(", ", movie.getGenres()) : "");
                    row.createCell(2).setCellValue(movie.getReleaseDate() != null ? movie.getReleaseDate() : "");
                }

                // Auto-size columns
                for (int i = 0; i < headers.length; i++) {
                    sheet.autoSizeColumn(i);
                }

                batchNum++;
            }

            // Write to file
            try (FileOutputStream out = new FileOutputStream(filename)) {
                workbook.write(out);
            }

            System.out.println("Excel file with " + batches.size() + " batches written: " + filename);
            return filename;

        } catch (IOException e) {
            System.err.println("Error writing Excel file: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}

