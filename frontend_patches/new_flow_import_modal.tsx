import React, { useState } from "react";

export type NewFlowImportMode =
  | "upload_from_computer"
  | "add_from_stage"
  | "add_from_git";

export interface StageFlowImportInput {
  stageUri: string;
  fileName: string;
}

export interface GitFlowImportInput {
  repoUrl: string;
  gitRef: string;
  commitSha: string;
  filePath: string;
  eaiName: string;
  secretRef: string;
}

export interface NewFlowImportModalProps {
  activeRole: string;
  authorizedStages: string[];
  onUploadFromComputer: (fileName: string, jsonContent: string) => Promise<void>;
  onAddFromStage: (input: StageFlowImportInput) => Promise<void>;
  onAddFromGit: (input: GitFlowImportInput) => Promise<void>;
}

/**
 * New Flow Import Modal (REQ-03):
 * Supports `Upload from computer`, `Add from stage`, and `Add from Git` under active role governance,
 * with schema validation, secret redaction, and zero automatic custom code execution on import.
 */
export const NewFlowImportModal: React.FC<NewFlowImportModalProps> = ({
  activeRole,
  authorizedStages,
  onUploadFromComputer,
  onAddFromStage,
  onAddFromGit,
}) => {
  const [mode, setMode] = useState<NewFlowImportMode>("upload_from_computer");
  const [selectedStage, setSelectedStage] = useState<string>(
    authorizedStages[0] || "@<DATABASE>.<SCHEMA>.<STAGE_NAME>"
  );
  const [stageSubpath, setStageSubpath] = useState<string>("flows/flow.json");
  const [gitRepoUrl, setGitRepoUrl] = useState<string>("");
  const [gitRef, setGitRef] = useState<string>("main");
  const [commitSha, setCommitSha] = useState<string>("");
  const [gitFilePath, setGitFilePath] = useState<string>("flows/flow.json");
  const [eaiName, setEaiName] = useState<string>("<GIT_EAI_NAME>");
  const [secretRef, setSecretRef] = useState<string>("<DATABASE>.<SCHEMA>.<GIT_SECRET>");

  return (
    <div
      data-testid="new-flow-import-modal"
      data-active-role={activeRole}
      className="flex flex-col gap-4 rounded-lg border border-border bg-background p-6 text-sm text-foreground shadow-sm"
    >
      <div className="flex items-center justify-between border-b border-border pb-3">
        <h2 className="text-base font-semibold">New Flow — Import</h2>
        <span className="text-xs text-muted-foreground">
          Active Role: <strong>{activeRole}</strong>
        </span>
      </div>

      {/* Import Mode Selector Tabs */}
      <div className="flex gap-2" role="tablist" aria-label="New Flow Import Options">
        <button
          type="button"
          role="tab"
          aria-selected={mode === "upload_from_computer"}
          data-testid="tab-upload-from-computer"
          onClick={() => setMode("upload_from_computer")}
          className="rounded-md border border-border px-3 py-1.5 text-xs font-medium"
        >
          Upload from computer
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === "add_from_stage"}
          data-testid="tab-add-from-stage"
          onClick={() => setMode("add_from_stage")}
          className="rounded-md border border-border px-3 py-1.5 text-xs font-medium"
        >
          Add from stage
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={mode === "add_from_git"}
          data-testid="tab-add-from-git"
          onClick={() => setMode("add_from_git")}
          className="rounded-md border border-border px-3 py-1.5 text-xs font-medium"
        >
          Add from Git
        </button>
      </div>

      {mode === "upload_from_computer" && (
        <div data-testid="panel-upload-from-computer" className="flex flex-col gap-3">
          <p className="text-xs text-muted-foreground">
            Upload a Langflow JSON file. Embedded secrets are redacted and custom code is never executed automatically on import.
          </p>
          <input
            type="file"
            accept=".json,application/json"
            data-testid="input-upload-flow-json"
            onChange={async (e) => {
              const file = e.target.files?.[0];
              if (file) {
                const text = await file.text();
                await onUploadFromComputer(file.name, text);
              }
            }}
          />
        </div>
      )}

      {mode === "add_from_stage" && (
        <div data-testid="panel-add-from-stage" className="flex flex-col gap-3">
          <label className="flex flex-col gap-1 text-xs">
            <span>Authorized Snowflake Stage (filtered by role {activeRole})</span>
            <select
              data-testid="select-snowflake-stage"
              value={selectedStage}
              onChange={(e) => setSelectedStage(e.target.value)}
              className="rounded border border-border bg-background px-2 py-1.5"
            >
              {authorizedStages.map((stg) => (
                <option key={stg} value={stg}>
                  {stg}
                </option>
              ))}
            </select>
          </label>
          <label className="flex flex-col gap-1 text-xs">
            <span>Stage JSON Sub-path</span>
            <input
              type="text"
              data-testid="input-stage-subpath"
              value={stageSubpath}
              onChange={(e) => setStageSubpath(e.target.value)}
              className="rounded border border-border bg-background px-2 py-1.5"
            />
          </label>
          <button
            type="button"
            data-testid="btn-submit-stage-import"
            onClick={() =>
              onAddFromStage({
                stageUri: `${selectedStage}/${stageSubpath.replace(/^\/+/, "")}`,
                fileName: stageSubpath,
              })
            }
            className="self-end rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground"
          >
            Import from Stage
          </button>
        </div>
      )}

      {mode === "add_from_git" && (
        <div data-testid="panel-add-from-git" className="flex flex-col gap-3">
          <input
            type="url"
            placeholder="https://github.com/<ORG>/<REPO>.git"
            data-testid="input-git-repo-url"
            value={gitRepoUrl}
            onChange={(e) => setGitRepoUrl(e.target.value)}
            className="rounded border border-border bg-background px-2 py-1.5 text-xs"
          />
          <div className="grid grid-cols-2 gap-2">
            <input
              type="text"
              placeholder="Branch or Tag (e.g. main)"
              data-testid="input-git-ref"
              value={gitRef}
              onChange={(e) => setGitRef(e.target.value)}
              className="rounded border border-border bg-background px-2 py-1.5 text-xs"
            />
            <input
              type="text"
              placeholder="Commit SHA"
              data-testid="input-git-commit-sha"
              value={commitSha}
              onChange={(e) => setCommitSha(e.target.value)}
              className="rounded border border-border bg-background px-2 py-1.5 text-xs"
            />
          </div>
          <input
            type="text"
            placeholder="Path to .json flow file"
            data-testid="input-git-file-path"
            value={gitFilePath}
            onChange={(e) => setGitFilePath(e.target.value)}
            className="rounded border border-border bg-background px-2 py-1.5 text-xs"
          />
          <button
            type="button"
            data-testid="btn-submit-git-import"
            onClick={() =>
              onAddFromGit({
                repoUrl: gitRepoUrl,
                gitRef,
                commitSha,
                filePath: gitFilePath,
                eaiName,
                secretRef,
              })
            }
            className="self-end rounded-md bg-primary px-3 py-1.5 text-xs font-medium text-primary-foreground"
          >
            Import from Git
          </button>
        </div>
      )}
    </div>
  );
};
