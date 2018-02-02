package com.castandcrew.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfReader;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.AreaBreak;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.property.AreaBreakType;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;

public class PdfService {
    private static final Logger LOG = Logger.getLogger(PdfService.class);
    private AWSS3Service service;

    public PdfService(AWSS3Service service) {
        this.service = service;
    }

    public void putObject(S3Object object, final String targetBucket) throws IOException {
        ByteArrayInputStream inputStream = processRequest(object);
        logMessage("Adding meta data");
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.addUserMetadata("Writer", "Anirudh Karanam");
        metadata.setLastModified(new Date());
        final String fileName = "target_" + object.getKey();

        logMessage(String.format("Copying to bucket: %s, key: %s", targetBucket, fileName));
//        boolean doesBucketExist = service.doesBucketExists(targetBucket);
//        logMessage("Bucket Exists?" + doesBucketExist);
//        if (doesBucketExist) {
            service.transerFile(inputStream, metadata, targetBucket, fileName);
//        }
    }

    private ByteArrayInputStream processRequest(S3Object object) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            logMessage("Create PDFDocument");
            PdfDocument pdfDocument = new PdfDocument(new PdfReader(object.getObjectContent()), new PdfWriter(outputStream));
            logMessage("Updating the Document!");
            Document doc = new Document(pdfDocument);
            AreaBreak pageBreak = new AreaBreak(AreaBreakType.NEXT_PAGE);
            pageBreak.setPageSize(new PageSize(pdfDocument.getFirstPage().getPageSize()));
            doc.add(pageBreak);
            doc.add(new Paragraph("Some other Text here"));

            pdfDocument.close();
        } catch (Exception e) {
            logMessage("Error with iText: " + e.getMessage());
            throw  e;
        }

        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    private void logMessage(String message) {
        LOG.info("PDFKit - " + message);
    }
}
