package pt.com.broker;

public class EndpointReference
{
	/*
	 
	<wsa:EndpointReference>
    	<wsa:Address>xs:anyURI</wsa:Address>
    	<wsa:ReferenceParameters>xs:any*</wsa:ReferenceParameters> ?
    	<wsa:Metadata>xs:any*</wsa:Metadata>?
	</wsa:EndpointReference>
	 
	 */
	
	public String Address;
	public String ReferenceParameters;
	public String Metadata;

}
