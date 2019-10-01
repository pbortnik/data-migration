/*
 * Copyright 2018 EPAM Systems
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

package com.epam.reportportal.migration.datastore.filesystem.distributed;

import com.epam.reportportal.migration.datastore.filesystem.DataStore;
import org.lokra.seaweedfs.core.FileSource;
import org.lokra.seaweedfs.core.FileTemplate;
import org.lokra.seaweedfs.core.file.FileHandleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Dzianis_Shybeka
 */
public class SeaweedDataStore implements DataStore {

	private static final Logger logger = LoggerFactory.getLogger(SeaweedDataStore.class);

	private final FileSource fileSource;

	public SeaweedDataStore(FileSource fileSource) {
		this.fileSource = fileSource;
	}

	@Override
	public String save(String fileName, InputStream inputStream) {

		FileTemplate fileTemplate = new FileTemplate(fileSource.getConnection());
		try {
			FileHandleStatus fileHandleStatus = fileTemplate.saveFileByStream(fileName, inputStream);

			return fileHandleStatus.getFileId();
		} catch (IOException e) {
			logger.error("Unable to save log file ", e);
			throw new RuntimeException("Unable to save log file");
		}
	}
}
