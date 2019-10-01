/*
 * Copyright 2019 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.epam.reportportal.migration.datastore.binary.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author <a href="mailto:ihar_kahadouski@epam.com">Ihar Kahadouski</a>
 */
public class DataStoreUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreUtils.class);

	private static final String THUMBNAIL_PREFIX = "thumbnail-";

	static final String ROOT_USER_PHOTO_DIR = "users";

	static final String ATTACHMENT_CONTENT_TYPE = "attachmentContentType";

	private DataStoreUtils() {
		//static only
	}

	public static String buildThumbnailFileName(String commonPath, String fileName) {
		Path thumbnailTargetPath = Paths.get(commonPath, THUMBNAIL_PREFIX.concat(fileName));
		return thumbnailTargetPath.toString();
	}
}
