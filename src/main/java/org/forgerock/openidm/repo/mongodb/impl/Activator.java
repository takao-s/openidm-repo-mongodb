/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright © 2011 ForgeRock AS.
 * Portions Copyrighted 2013 Takao Sekiguchi.
 * All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openidm.repo.mongodb.impl;

import java.util.Hashtable;

import org.forgerock.json.fluent.JsonValue;
import org.forgerock.openidm.config.persistence.ConfigBootstrapHelper;
import org.forgerock.openidm.repo.RepoBootService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.osgi.framework.Constants;

/**
 * OSGi bundle activator
 * @author takao-s
 */
public class Activator implements BundleActivator {
    final static Logger logger = LoggerFactory.getLogger(Activator.class);

     public void start(BundleContext context) {
         logger.debug("MongoDB bundle starting", context);
         
         JsonValue repoConfig = ConfigBootstrapHelper.getRepoBootConfig("mongodb", context);
         
         if (repoConfig != null) {
             logger.info("Bootstrapping MongoDB repository");
             // Init the bootstrap repo
             RepoBootService bootSvc = MongoDBRepoService.getRepoBootService(repoConfig);
             
             // Register bootstrap repo
             Hashtable<String, String> prop = new Hashtable<String, String>();
             prop.put(Constants.SERVICE_PID, "org.forgerock.openidm.bootrepo.mongodb");
             prop.put("openidm.router.prefix", "bootrepo");
             prop.put("db.type", "MongoDB");
             context.registerService(RepoBootService.class.getName(), bootSvc, prop);
             logger.info("Registered bootstrap repository service");
         } else {
             logger.debug("No MongoDB configuration detected");
         }
         logger.debug("MongoDB bundle started", context);
     }

     public void stop(BundleContext context) {
         logger.debug("MongoDB bundle stopped", context);
     }
}
