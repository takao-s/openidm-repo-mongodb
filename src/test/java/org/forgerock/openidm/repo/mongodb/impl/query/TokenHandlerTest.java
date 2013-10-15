package org.forgerock.openidm.repo.mongodb.impl.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.forgerock.openidm.objset.BadRequestException;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * TokenHandler test
 * 
 * @author takao-s
 */
public class TokenHandlerTest {
    Map<String, Object> params = new HashMap<String, Object>();
    
    @BeforeTest
    public void init() {
        params.put("gt", "foo");
        params.put("age", Integer.valueOf(20));
        params.put("values", new ArrayList<String>(){{add("1000"); add("2000");}});
        params.put("names", new ArrayList<String>(){{add("uid"); add("cn"); add("sn");}});
        params.put("name", new ArrayList<String>(){{add("uid");}});
        params.put("field", "user/familyName");
        params.put("lt", "bar");
    }
    
    @Test
    public void testReplaceTokensWithValues() {
        TokenHandler th = new TokenHandler();
        String queryString = "{ \"age\" : { \"$gt\" : \"${age}\" }}";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{ \"age\" : { \"$gt\" : \"20\" }}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_unquoted() {
        TokenHandler th = new TokenHandler();
        String queryString = "{ \"age\" : { \"$gt\" : \"${unquoted:age}\" }}";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{ \"age\" : { \"$gt\" : 20 }}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_array() {
        TokenHandler th = new TokenHandler();
        String queryString = "{ \"sallary\" : { \"$in\" : [\"${values}\"] }}";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{ \"sallary\" : { \"$in\" : [\"1000\",\"2000\"] }}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_unquoted_array() {
        TokenHandler th = new TokenHandler();
        String queryString = "{ \"sallary\" : { \"$in\" : [\"${unquoted:values}\"] }}";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{ \"sallary\" : { \"$in\" : [1000,2000] }}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_fields() {
        TokenHandler th = new TokenHandler();
        String queryString = "\"${fields:names}\"";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{\"uid\":true,\"cn\":true,\"sn\":true}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_fields_one() {
        TokenHandler th = new TokenHandler();
        String queryString = "\"${fields:name}\"";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{\"uid\":true}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
    
    @Test
    public void testReplaceTokensWithValues_dotnotation() {
        TokenHandler th = new TokenHandler();
        String queryString = "{ \"${dotnotation:field}\" : \"Smith\" }}";
        
        try {
            String res = th.replaceTokensWithValues(queryString, params);
            
            Assert.assertEquals(res, "{ \"user.familyName\" : \"Smith\" }}");
            return;
        } catch (BadRequestException e) {
            Assert.fail(e.getMessage());
        }
        Assert.fail("Not yet implemented");
    }
}
