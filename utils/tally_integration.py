# tally_integration.py

def get_ledger_data():
    xml_request = """
    <ENVELOPE>
      <HEADER>
        <TALLYREQUEST>Export Data</TALLYREQUEST>
      </HEADER>
      <BODY>
        <EXPORTDATA>
          <REQUESTDESC>
            <REPORTNAME>Ledger Vouchers</REPORTNAME>
            <STATICVARIABLES>
              <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
              <LEDGERNAME>Customer A</LEDGERNAME>
              <SVFROMDATE>20240401</SVFROMDATE>
              <SVTODATE>20250331</SVTODATE>
            </STATICVARIABLES>
          </REQUESTDESC>
        </EXPORTDATA>
      </BODY>
    </ENVELOPE>
    """
    print("📦 XML Request Prepared to send to Tally:")
    print(xml_request)
    print("\n✅ (This is a simulation. In real implementation, this would hit http://localhost:9000)")

    # Simulate a response
    return {"Customer A": [{"Date": "01-Apr-2024", "Amount": 10000}]}

if __name__ == "__main__":
    data = get_ledger_data()
    print("🔍 Sample Parsed Data:", data)
