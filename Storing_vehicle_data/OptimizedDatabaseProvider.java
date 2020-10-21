
package de.entwicklerheld.sqlfinchallenge.challenge.stage2;

import de.entwicklerheld.sqlfinchallenge.challenge.IDatabaseProvider;

import java.sql.*;

public class OptimizedDatabaseProvider extends DatabaseProvider implements IDatabaseProvider {

    @Override
    public void createVINTable(Connection connection) throws SQLException {
        // The naive solution. You can change it as you like
        Statement statement = connection.createStatement();
        statement.execute(
                "CREATE TABLE IF NOT EXISTS vindata (\n" +
                        " id INTEGER NOT NULL PRIMARY KEY,\n" +
                        " vin1 TEXT(11) NOT NULL, \n" +
                        " vin2 INTEGER(6) NOT NULL, \n" +
                        " data BLOB NOT NULL\n" +
                        ");"
        );
    }

    @Override
    public void insertVINData(Connection connection, String vin, byte[] data) throws SQLException {
        if(isVinValid(vin))
        {
            String sql = "INSERT INTO vindata(vin1, vin2, data) VALUES(?, ?, ?)";
            PreparedStatement pstmt = connection.prepareStatement(sql);

            pstmt.setString(1, vin.substring(0, 11));
            pstmt.setInt(2, convertVinToInt(vin)); 
            pstmt.setBytes(3, data);

            pstmt.executeUpdate(); 
        }
    }

    @Override
    public byte[] getVINData(Connection connection, String vin) throws SQLException {
        byte[] data = null;

        if(isVinValid(vin))
        {
            int convertedVin = convertVinToInt(vin);

            String sql = "SELECT data FROM vindata WHERE vin1=? AND  vin2=?";
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, vin.substring(0,11));
            pstmt.setInt(2, convertedVin);

            ResultSet rs = pstmt.executeQuery();
            data = rs.getBytes("data");
        }
        
        return data;
    }
    

    private int convertVinToInt(String vin)
    {
        String significantDigits = vin.substring(11,17);
        int convertedVin = 0;
        int maxPower = 5; 
        
        for(int i =  0; i < significantDigits.length(); ++i)
        {
            int convertedCharacter = convertCharToExtendedHex(significantDigits.charAt(i));
            convertedVin += convertedCharacter * (33 ^ (maxPower - i)); // add converted Character with appropriate power to converted vin 
        }

        return convertedVin;
    }

    private boolean isVinValid(String vin)
    {
        boolean correctLength = (vin.length() == 17) ? true : false;
        boolean containsIllegalArgs = (vin.contains("O") || vin.contains("I") || vin.contains("Q")) ? true : false;

        if(!correctLength || containsIllegalArgs)
        {
            return false;
        } 
        else
        {
            return true;
        } 
    }
    
    private int convertCharToExtendedHex(char character)
    {
        int asciiValue = (int)character; // convert character to ascii
        int extendedHex;
        
        if(asciiValue >= 65) // if ascii above or equal to 65 then character otherwise int
        {
            int offset = 55; //Offset come from A = 65 in ascii - 10 since that is the value for A in hex
            //calculate additional offset for characters that lie beyond the illegal characters
            if(asciiValue > 73)
            {
                offset += 1;
            }

            if(asciiValue > 79)
            {
                offset += 1;
            } 

            if(asciiValue > 81)
            {
                offset += 1;
            }

            extendedHex = asciiValue - offset;
        }
        else
        {
            int offset = 48; //Offset come from 0 = 48
            extendedHex = asciiValue - offset;
        }
    return extendedHex;
    }
}
