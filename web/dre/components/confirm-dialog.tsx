/**
 * ConfirmDialog — a non-blocking replacement for window.confirm / window.prompt.
 *
 * Usage (simple confirm):
 *   const { dialog, confirm } = useConfirmDialog();
 *   // In JSX: {dialog}
 *   // In handler: if (await confirm({ title: '…', message: '…' })) { … }
 *
 * Usage (with reason input):
 *   const { dialog, confirm } = useConfirmDialog();
 *   const result = await confirm({ title: '…', message: '…', requireReason: true, reasonLabel: 'Reason:' });
 *   if (result.confirmed) { doSomething(result.reason); }
 */

'use client';

import React, { useCallback, useRef, useState } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ConfirmOptions {
    title: string;
    message: string;
    confirmLabel?: string;
    cancelLabel?: string;
    variant?: 'danger' | 'warning' | 'info';
    /** If true, a text input is shown and the resolved value includes `reason`. */
    requireReason?: boolean;
    reasonLabel?: string;
    reasonPlaceholder?: string;
}

export interface ConfirmResult {
    confirmed: boolean;
    reason?: string;
}

// ---------------------------------------------------------------------------
// Hook
// ---------------------------------------------------------------------------

export function useConfirmDialog() {
    const [open, setOpen] = useState(false);
    const [opts, setOpts] = useState<ConfirmOptions | null>(null);
    const [reason, setReason] = useState('');
    const resolveRef = useRef<((result: ConfirmResult) => void) | null>(null);

    const confirm = useCallback((options: ConfirmOptions): Promise<ConfirmResult> => {
        return new Promise((resolve) => {
            setOpts(options);
            setReason('');
            setOpen(true);
            resolveRef.current = resolve;
        });
    }, []);

    const handleConfirm = useCallback(() => {
        setOpen(false);
        resolveRef.current?.({ confirmed: true, reason: reason.trim() });
        resolveRef.current = null;
    }, [reason]);

    const handleCancel = useCallback(() => {
        setOpen(false);
        resolveRef.current?.({ confirmed: false });
        resolveRef.current = null;
    }, []);

    const variantStyles: Record<string, string> = {
        danger: 'bg-red-600 hover:bg-red-700 focus:ring-red-500',
        warning: 'bg-amber-500 hover:bg-amber-600 focus:ring-amber-400',
        info: 'bg-blue-600 hover:bg-blue-700 focus:ring-blue-500',
    };

    const iconByVariant: Record<string, React.ReactNode> = {
        danger: (
            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-red-100 dark:bg-red-900/30">
                <svg className="h-6 w-6 text-red-600 dark:text-red-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
                </svg>
            </div>
        ),
        warning: (
            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-amber-100 dark:bg-amber-900/30">
                <svg className="h-6 w-6 text-amber-600 dark:text-amber-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126ZM12 15.75h.007v.008H12v-.008Z" />
                </svg>
            </div>
        ),
        info: (
            <div className="mx-auto flex h-12 w-12 items-center justify-center rounded-full bg-blue-100 dark:bg-blue-900/30">
                <svg className="h-6 w-6 text-blue-600 dark:text-blue-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="m11.25 11.25.041-.02a.75.75 0 0 1 1.063.852l-.708 2.836a.75.75 0 0 0 1.063.853l.041-.021M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Zm-9-3.75h.008v.008H12V8.25Z" />
                </svg>
            </div>
        ),
    };

    const variant = opts?.variant ?? 'info';
    const btnClass = variantStyles[variant] ?? variantStyles.info;
    const icon = iconByVariant[variant] ?? iconByVariant.info;
    const confirmLabel = opts?.confirmLabel ?? (variant === 'danger' ? 'Delete' : 'Confirm');
    const cancelLabel = opts?.cancelLabel ?? 'Cancel';

    const dialog = open && opts ? (
        <div
            className="fixed inset-0 z-[9999] flex items-center justify-center"
            role="dialog"
            aria-modal="true"
            aria-labelledby="confirm-dialog-title"
        >
            {/* Backdrop */}
            <div
                className="absolute inset-0 bg-black/50 backdrop-blur-sm"
                onClick={handleCancel}
            />

            {/* Panel */}
            <div className="relative z-10 w-full max-w-md rounded-xl bg-white dark:bg-slate-900 shadow-2xl ring-1 ring-slate-200 dark:ring-slate-700 p-6 mx-4">
                <div className="text-center">
                    {icon}
                    <h3
                        id="confirm-dialog-title"
                        className="mt-4 text-base font-semibold text-slate-900 dark:text-slate-100"
                    >
                        {opts.title}
                    </h3>
                    <p className="mt-2 text-sm text-slate-600 dark:text-slate-400 whitespace-pre-wrap">
                        {opts.message}
                    </p>
                </div>

                {opts.requireReason && (
                    <div className="mt-4">
                        <label
                            htmlFor="confirm-dialog-reason"
                            className="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1"
                        >
                            {opts.reasonLabel ?? 'Reason (required)'}
                        </label>
                        <textarea
                            id="confirm-dialog-reason"
                            rows={3}
                            value={reason}
                            onChange={(e) => setReason(e.target.value)}
                            placeholder={opts.reasonPlaceholder ?? 'Enter a reason…'}
                            className="w-full rounded-lg border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm text-slate-900 dark:text-slate-100 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                        />
                    </div>
                )}

                <div className="mt-6 flex gap-3 justify-end">
                    <button
                        type="button"
                        onClick={handleCancel}
                        className="rounded-lg border border-slate-300 dark:border-slate-600 px-4 py-2 text-sm font-medium text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-400 transition-colors"
                    >
                        {cancelLabel}
                    </button>
                    <button
                        type="button"
                        onClick={handleConfirm}
                        disabled={opts.requireReason && !reason.trim()}
                        className={`rounded-lg px-4 py-2 text-sm font-medium text-white focus:outline-none focus:ring-2 focus:ring-offset-2 transition-colors disabled:opacity-40 disabled:cursor-not-allowed ${btnClass}`}
                    >
                        {confirmLabel}
                    </button>
                </div>
            </div>
        </div>
    ) : null;

    return { dialog, confirm };
}
