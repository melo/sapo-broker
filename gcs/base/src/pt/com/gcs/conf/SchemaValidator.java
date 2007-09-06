package pt.com.gcs.conf;

import java.io.File;
import java.io.IOException;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.SAXException;

public class SchemaValidator
{
	public static XsdValidationResult validate(Source schemaLocation, File xmlFile)
	{
		// 1. Lookup a factory for the W3C XML Schema language
		SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");

		// 2. Compile the schema.
		Schema schema;
		try
		{
			schema = factory.newSchema(schemaLocation);
		}
		catch (SAXException e)
		{
			throw new RuntimeException(e);
		}

		// 3. Get a validator from the schema.
		Validator validator = schema.newValidator();

		// 4. Parse the document you want to check.
		Source source = new StreamSource(xmlFile);

		// 5. Check the document
		XsdValidationResult result;
		try
		{
			validator.validate(source);
			result = new XsdValidationResult(true, xmlFile.getName() + " is valid.");
		}
		catch (SAXException ex)
		{
			result = new XsdValidationResult(false, xmlFile.getName() + " is not valid because: " + ex.getMessage());
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}

		return result;
	}
}