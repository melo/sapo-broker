<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
        version="1.0">
<xsl:output method="xml" media-type="text/xml" indent="no" omit-xml-declaration="yes"  encoding="utf-8" />

<xsl:template match="/">
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <soap:Body>
        <xsl:apply-templates/>
   </soap:Body>
</soap:Envelope>
</xsl:template>

<xsl:template match="/*[1]">
	<xsl:element name="{local-name(.)}" namespace="http://services.sapo.pt/definitions">       
			<xsl:apply-templates/>       
	</xsl:element>
</xsl:template>

<xsl:template match="*">
	<xsl:element name="{local-name(.)}" namespace="http://services.sapo.pt/definitions">
			<xsl:apply-templates/>
	</xsl:element>
</xsl:template> 

</xsl:stylesheet>